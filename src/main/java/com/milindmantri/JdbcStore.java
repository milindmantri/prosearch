package com.milindmantri;

import static java.lang.System.currentTimeMillis;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.util.Objects.requireNonNull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.norconex.collector.core.store.DataStoreException;
import com.norconex.collector.core.store.IDataStore;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * This is only a duplicate of original JdbcDataStore. Since, the original used setClob which is not
 * implemented in Postgres, it wasn't working.
 *
 * @param <T>
 */
public class JdbcStore<T> implements IDataStore<T> {

  // Used to parse HttpDocInfo
  private static final Gson GSON =
      new GsonBuilder()
          .registerTypeAdapter(ZoneId.class, new ZoneIdAdapter())
          .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeTypeAdapter())
          .create();

  private static final JdbcStore.PreparedStatementConsumer NO_ARGS = stmt -> {};
  static final String QUEUED_STORE = "queued";

  private final JdbcStoreEngine engine;
  private String tableName;
  private String storeName;
  private final Class<? extends T> type;
  private final ProsearchTableAdapter adapter;
  private final DomainCounter domainCounter;

  JdbcStore(
      JdbcStoreEngine engine,
      String storeName,
      Class<? extends T> type,
      final DomainCounter domainCounter) {
    super();
    this.engine = requireNonNull(engine, "'engine' must not be null.");
    this.type = requireNonNull(type, "'type' must not be null.");
    this.adapter = engine.getTableAdapter();
    this.storeName = requireNonNull(storeName, "'storeName' must not be null.");
    this.tableName = engine.tableName(storeName);
    this.domainCounter = requireNonNull(domainCounter, "domainCounter must not be null.");
    if (!engine.tableExist(tableName)) {
      createTable();
    }
  }

  @Override
  public String getName() {
    return storeName;
  }

  String tableName() {
    return tableName;
  }

  @Override
  public void save(String id, T object) {
    executeWrite(
        "MERGE INTO <table> AS t "
            + "USING ("
            + "  SELECT "
            + "    CAST(? AS "
            + adapter.idType()
            + ") AS id,"
            + "    CAST(? AS "
            + adapter.modifiedType()
            + ") AS modified,"
            + "    CAST(? AS "
            + adapter.jsonType()
            + ") AS json "
            + (isQueued() ? ", CAST(? AS VARCHAR) AS host" : "")
            // https://wiki.postgresql.org/wiki/Oracle_to_Postgres_Conversion#The_Dual_Table
            // + "  FROM DUAL"
            + ") AS s "
            + "  ON t.id = s.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (id, modified, "
            + (isQueued() ? "host, " : "")
            + "json) "
            + "  VALUES (s.id, s.modified, "
            + (isQueued() ? "s.host, " : "")
            + "s.json) "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET "
            + "    modified = s.modified, "
            + "    json = s.json ",
        stmt -> {
          String idValue = adapter.serializableId(id);
          stmt.setString(1, idValue);
          stmt.setTimestamp(2, new Timestamp(currentTimeMillis()));
          stmt.setString(3, GSON.toJson(object));

          if (isQueued()) {
            stmt.setString(4, new Host(URI.create(idValue)).toString());
          }
        });
  }

  @Override
  public Optional<T> find(String id) {
    return executeRead(
        "SELECT id, json FROM <table> WHERE id = ? LIMIT 1",
        stmt -> stmt.setString(1, adapter.serializableId(id)),
        this::firstObject);
  }

  @Override
  public Optional<T> findFirst() {
    return executeRead(
        "SELECT id, json FROM <table> ORDER BY modified LIMIT 1", NO_ARGS, this::firstObject);
  }

  @Override
  public boolean exists(String id) {
    return executeRead(
        "SELECT 1 FROM <table> WHERE id = ?",
        stmt -> stmt.setString(1, adapter.serializableId(id)),
        ResultSet::next);
  }

  @Override
  public long count() {
    return executeRead(
        "SELECT count(*) FROM <table>",
        NO_ARGS,
        rs -> {
          if (rs.next()) {
            return rs.getLong(1);
          }
          return 0L;
        });
  }

  @Override
  public boolean delete(String id) {
    return executeWrite(
            "DELETE FROM <table> WHERE id = ?",
            stmt -> stmt.setString(1, adapter.serializableId(id)))
        > 0;
  }

  @Override
  public Optional<T> deleteFirst() {

    if (isQueued()) {
      final Optional<Host> next = this.domainCounter.getNextHost();
      if (next.isPresent()) {

        final String sql = "SELECT id, json FROM <table> WHERE host = ? ORDER BY modified LIMIT 1";

        try (Connection conn = engine.getConnection();
            PreparedStatement stmt =
                conn.prepareStatement(
                    sql.replace("<table>", tableName),
                    // Requires scrollable type but wasn't set in original store
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    CONCUR_UPDATABLE)) {

          stmt.setString(1, next.get().toString());

          var rec = firstRecord(stmt.executeQuery());
          if (!rec.isEmpty()) {
            delete(rec.id);
            return rec.object;
          } else {
            domainCounter.notQueued(next.get());
          }

        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }

    JdbcStore.Record<T> rec =
        executeRead(
            "SELECT id, json FROM <table> ORDER BY modified LIMIT 1", NO_ARGS, this::firstRecord);
    if (!rec.isEmpty()) {
      delete(rec.id);

      return rec.object.filter(_ -> domainCounter.acceptHost(new Host(URI.create(rec.id))));
    }
    return rec.object;
  }

  @Override
  public void clear() {
    executeWrite("DELETE FROM <table>", NO_ARGS);
  }

  @Override
  public void close() {
    // NOOP: Closed implicitly when datasource is closed.
  }

  // returns true if was all read
  @Override
  public boolean forEach(BiPredicate<String, T> predicate) {
    return executeRead(
        "SELECT id, json FROM <table>",
        NO_ARGS,
        rs -> {
          while (rs.next()) {
            JdbcStore.Record<T> rec = toRecord(rs);
            if (!predicate.test(rec.id, rec.object.get())) {
              return false;
            }
          }
          return true;
        });
  }

  @Override
  public boolean isEmpty() {
    return executeRead("SELECT * FROM <table> LIMIT 1", NO_ARGS, (rs) -> !rs.next());
  }

  private void createTable() {
    try (Connection conn = engine.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(
            "CREATE TABLE "
                + tableName
                + " ("
                + "id "
                + adapter.idType()
                + " NOT NULL, "
                + (isQueued() ? "host VARCHAR NOT NULL, " : "")
                + "modified "
                + adapter.modifiedType()
                + ", "
                + "json "
                + adapter.jsonType()
                + ", "
                + "PRIMARY KEY (id) "
                + ")");
        stmt.executeUpdate(
            "CREATE INDEX " + tableName + "_modified_index " + "ON " + tableName + "(modified)");

        // fast index for polling queues
        if (isQueued()) {
          stmt.executeUpdate(
              "CREATE INDEX "
                  + tableName
                  + "_host_modified_index "
                  + "ON "
                  + tableName
                  + "(host, modified ASC)");
        }

        if (!conn.getAutoCommit()) {
          conn.commit();
        }
      }
    } catch (SQLException e) {
      throw new DataStoreException("Could not create table '" + tableName + "'.", e);
    }
  }

  boolean rename(String newStoreName) {
    String newTableName = engine.tableName(newStoreName);
    boolean targetExists = engine.tableExist(newTableName);
    if (targetExists) {
      executeWrite("DROP TABLE " + newTableName, NO_ARGS);
    }
    executeWrite("ALTER TABLE <table> RENAME TO " + newTableName, NO_ARGS);
    this.storeName = newStoreName;
    this.tableName = newTableName;
    return targetExists;
  }

  private Optional<T> firstObject(ResultSet rs) {
    try {
      if (rs.first()) {
        return toObject(rs.getString(2));
      }
      return Optional.empty();
    } catch (IOException | SQLException e) {
      throw new DataStoreException("Could not get object from table '" + tableName + "'.", e);
    }
  }

  private JdbcStore.Record<T> firstRecord(ResultSet rs) {
    try {
      if (rs.first()) {
        return toRecord(rs);
      }
      return new JdbcStore.Record<>();
    } catch (IOException | SQLException e) {
      throw new DataStoreException("Could not get record from table '" + tableName + "'.", e);
    }
  }

  private JdbcStore.Record<T> toRecord(ResultSet rs) throws IOException, SQLException {
    JdbcStore.Record<T> rec = new JdbcStore.Record<>();
    rec.id = rs.getString(1);
    rec.object = toObject(rs.getString(2));
    return rec;
  }

  private Optional<T> toObject(String str) throws IOException {
    return Optional.ofNullable(GSON.fromJson(str, type));
  }

  Class<?> getType() {
    return type;
  }

  private <R> R executeRead(
      String sql, JdbcStore.PreparedStatementConsumer psc, JdbcStore.ResultSetFunction<R> rsc) {
    try (Connection conn = engine.getConnection()) {
      try (PreparedStatement stmt =
          conn.prepareStatement(
              sql.replace("<table>", tableName),
              // Requires scrollable type but wasn't set in original store
              ResultSet.TYPE_SCROLL_INSENSITIVE,
              CONCUR_UPDATABLE)) {
        psc.accept(stmt);
        try (ResultSet rs = stmt.executeQuery()) {
          return rsc.accept(rs);
        }
      }
    } catch (SQLException | IOException e) {
      throw new DataStoreException("Could not read from table '" + tableName + "'.", e);
    }
  }

  private int executeWrite(String sql, JdbcStore.PreparedStatementConsumer c) {
    try (Connection conn = engine.getConnection()) {
      try (PreparedStatement stmt = conn.prepareStatement(sql.replace("<table>", tableName))) {
        c.accept(stmt);
        int val = stmt.executeUpdate();
        if (!conn.getAutoCommit()) {
          conn.commit();
        }
        return val;
      }
    } catch (SQLException e) {
      throw new DataStoreException("Could not write to table '" + tableName + "'.", e);
    }
  }

  @FunctionalInterface
  interface PreparedStatementConsumer {

    void accept(PreparedStatement stmt) throws SQLException;
  }

  @FunctionalInterface
  interface ResultSetFunction<R> {

    R accept(ResultSet rs) throws SQLException, IOException;
  }

  private static class Record<T> {

    private String id;
    private Optional<T> object = Optional.empty();

    private boolean isEmpty() {
      return id == null;
    }
  }

  private boolean isQueued() {
    return QUEUED_STORE.equals(this.storeName);
  }
}
