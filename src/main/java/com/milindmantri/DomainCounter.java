package com.milindmantri;

import com.norconex.collector.core.crawler.CrawlerEvent;
import com.norconex.collector.core.filter.IReferenceFilter;
import com.norconex.commons.lang.event.Event;
import com.norconex.commons.lang.event.IEventListener;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

public class DomainCounter implements IReferenceFilter, IEventListener<Event> {

  private static final String CREATE_TABLE =
      """
  CREATE TABLE IF NOT EXISTS
    host_count
  (
      host VARCHAR
    , url VARCHAR
  )
  """;

  private static final String PG_UNDEFINED_RELATION_ERR_CODE = "42P01";

  private final int limit;

  private final Map<String, AtomicInteger> count = new ConcurrentHashMap<>();

  private final DataSource dataSource;

  /** Caller is responsible for closing dataSource */
  public DomainCounter(final int limit, final DataSource dataSource) throws SQLException {
    if (limit <= 0) {
      throw new IllegalArgumentException(
          "Limit must be greater than zero, but was %d.".formatted(limit));
    }

    if (dataSource == null) {
      throw new IllegalArgumentException("{dataSource} must not be null.");
    }

    this.limit = limit;
    this.dataSource = dataSource;

    restoreCount();
  }

  @Override
  public boolean acceptReference(final String reference) {
    String host = URI.create(reference).getHost();

    // TODO: insertIntoDb and local count handling should happen in a txn

    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() == limit) {
        return false;
      } else {
        insertIntoDb(host, reference);
        return i.incrementAndGet() <= limit;
      }

    } else {
      insertIntoDb(host, reference);

      count.put(host, new AtomicInteger(1));
      return true;
    }
  }

  @Override
  public void accept(final Event event) {

    if (event.is(CrawlerEvent.CRAWLER_INIT_BEGIN)) {
      // create table

      try (var con = this.dataSource.getConnection();
          var createTablePs = con.prepareStatement(CREATE_TABLE);
          var indexStmt = con.createStatement()) {

        createTablePs.executeUpdate();
        indexStmt.executeUpdate("CREATE INDEX host_count_index ON host_count (host)");

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (event.is(CrawlerEvent.CRAWLER_RUN_END)) {
      try (var con = this.dataSource.getConnection();
          // TODO: IF EXISTS?
          var createTablePs = con.prepareStatement("DROP TABLE host_count")) {

        createTablePs.executeUpdate();

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void restoreCount() throws SQLException {

    try (var con = this.dataSource.getConnection();
        var ps =
            con.prepareStatement(
                """
            SELECT
              count(*), host
            FROM
              host_count
            GROUP BY host
          """)) {

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        int count = rs.getInt(1);
        String host = rs.getString(2);

        this.count.put(host, new AtomicInteger(count));
      }
    } catch (final SQLException ex) {
      // check if table exists
      if (!PG_UNDEFINED_RELATION_ERR_CODE.equals(ex.getSQLState())) {
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        // table exists, but fetch failed. Don't throw if table doesn't exist.

        throw ex;
      }
    }
  }

  private void insertIntoDb(final String host, final String reference) {
    try (var con = this.dataSource.getConnection();
        var ps = con.prepareStatement("INSERT INTO host_count(host, url) VALUES (?, ?)")) {

      ps.setString(1, host);
      ps.setString(2, reference);

      ps.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
