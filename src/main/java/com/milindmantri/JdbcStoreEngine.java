package com.milindmantri;

import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;

import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.store.DataStoreException;
import com.norconex.collector.core.store.IDataStore;
import com.norconex.collector.core.store.IDataStoreEngine;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.IXMLConfigurable;
import com.norconex.commons.lang.xml.XML;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data store engine using a JDBC-compatible database for storing crawl data.
 *
 * <h3>Database JDBC driver</h3>
 *
 * <p>To use this data store engine, you need its JDBC database driver on the classpath.
 *
 * <h3>Database datasource configuration</h3>
 *
 * <p>This JDBC data store engine uses <a
 * href="https://github.com/brettwooldridge/HikariCP">Hikari</a> as the JDBC datasource
 * implementation, which provides efficient connection-pooling. Refer to <a
 * href="https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby">Hikari's
 * documentation</a> for all configuration options. The Hikari options are passed as-is, via <code>
 * datasource</code> properties as shown below.
 *
 * <h3>Data types</h3>
 *
 * <p>This class only use a few data types to store its data in a generic way. It will try to detect
 * what data type to use for your database. If you get errors related to field data types not being
 * supported, you have the option to redefined them.
 *
 * <p>{@nx.xml.usage <dataStoreEngine
 * class="com.norconex.collector.core.store.impl.jdbc.JdbcDataStoreEngine"> <!-- Hikari datasource
 * configuration properties: --> <datasource> <property name="(property name)">(property
 * value)</property> </datasource> <tablePrefix> (Optional prefix used for table creation. Default
 * is the collector id plus the crawler id, each followed by an underscore character.)
 * </tablePrefix> <!-- Optionally overwrite default SQL data type used. You should only use if you
 * get data type-related errors. --> <dataTypes> <varchar use="(equivalent data type for your
 * database)" /> <timestamp use="(equivalent data type for your database)" /> <text use="(equivalent
 * data type for your database)" /> </dataTypes> </dataStoreEngine> }
 *
 * <p>{@nx.xml.example <dataStoreEngine class="JdbcDataStoreEngine"> <datasource> <property
 * name="jdbcUrl">jdbc:mysql://localhost:33060/sample</property> <property
 * name="username">dbuser</property> <property name="password">dbpwd</property> <property
 * name="connectionTimeout">1000</property> </datasource> </dataStoreEngine> }
 *
 * <p>The above example contains basic settings for creating a MySQL data source.
 *
 * @author Pascal Essiembre (original author)
 *     <p>This is a copy of JdbcDataStoreEngine edited to make things work with postgres
 */
public class JdbcStoreEngine implements IDataStoreEngine, IXMLConfigurable {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcStoreEngine.class);

  private static final String STORE_TYPES_NAME = "_storetypes";

  // Non-configurable:
  private HikariDataSource datasource;
  private String tablePrefix;
  // table id field is store name
  private JdbcStore<String> storeTypes;
  private ProsearchTableAdapter tableAdapter;

  // Configurable:
  private Properties configProperties = new Properties();
  private String varcharType;
  private String timestapType;
  private String textType;
  private final Manager manager;

  public JdbcStoreEngine(Manager manager) {
    this.manager = Objects.requireNonNull(manager, "domainCounter must not be null.");
  }

  public Properties getConfigProperties() {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties) {
    this.configProperties = configProperties;
  }

  public String getTablePrefix() {
    return tablePrefix;
  }

  public void setTablePrefix(String tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  public String getVarcharType() {
    return varcharType;
  }

  public void setVarcharType(String varcharType) {
    this.varcharType = varcharType;
  }

  public String getTimestapType() {
    return timestapType;
  }

  public void setTimestapType(String timestapType) {
    this.timestapType = timestapType;
  }

  public String getTextType() {
    return textType;
  }

  public void setTextType(String textType) {
    this.textType = textType;
  }

  @Override
  public void init(Crawler crawler) {
    // create a clean table name prefix to avoid collisions in case
    // multiple crawlers use the same DB.
    if (this.tablePrefix == null) {
      this.tablePrefix = crawler.getCollector().getId() + "_" + crawler.getId() + "_";
    }

    // create data source
    datasource = new HikariDataSource(new HikariConfig(configProperties.toProperties()));

    tableAdapter = resolveTableAdapter();

    // store types for each table
    storeTypes = new JdbcStore<>(this, STORE_TYPES_NAME, String.class, this.manager);
  }

  private ProsearchTableAdapter resolveTableAdapter() {
    return ProsearchTableAdapter.detect(
            StringUtils.firstNonBlank(datasource.getJdbcUrl(), datasource.getDriverClassName()))
        .withIdType(varcharType)
        .withModifiedType(timestapType)
        .withJsonType(textType);
  }

  @Override
  public boolean clean() {
    // the table storing the store types is not returned by getStoreNames
    // so we have to explicitly delete it.
    Set<String> names = getStoreNames();
    boolean hasStores = !names.isEmpty();
    if (hasStores) {
      names.stream().forEach(this::dropStore);
    }
    dropStore(STORE_TYPES_NAME);
    return hasStores;
  }

  @Override
  public void close() {
    if (datasource != null) {
      LOG.info("Closing JDBC data store engine datasource...");
      datasource.close();
      LOG.info("JDBC Data store engine datasource closed.");
      datasource = null;
    } else {
      LOG.info("JDBC Data store engine datasource already closed.");
    }
  }

  @Override
  public <T> IDataStore<T> openStore(String storeName, Class<? extends T> type) {
    storeTypes.save(storeName, type.getName());
    return new JdbcStore<>(this, storeName, type, this.manager);
  }

  @Override
  public boolean dropStore(String storeName) {
    String tableName = tableName(storeName);
    if (!tableExist(tableName)) {
      return false;
    }
    try (Connection conn = datasource.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DROP TABLE " + tableName);
        if (!conn.getAutoCommit()) {
          conn.commit();
        }
      }
    } catch (SQLException e) {
      throw new DataStoreException("Could not drop table '" + tableName + "'.", e);
    }

    if (storeName.equals(STORE_TYPES_NAME)) {
      storeTypes = null;
    } else {
      storeTypes.delete(storeName);
    }
    return true;
  }

  @Override
  public boolean renameStore(IDataStore<?> dataStore, String newStoreName) {
    JdbcStore<?> jdbcStore = (JdbcStore<?>) dataStore;
    String oldStoreName = jdbcStore.getName();
    boolean existed = ((JdbcStore<?>) dataStore).rename(newStoreName);
    storeTypes.delete(oldStoreName);
    storeTypes.save(newStoreName, jdbcStore.getType().getName());
    return existed;
  }

  @Override
  public Set<String> getStoreNames() {
    Set<String> names = new HashSet<>();
    try (Connection conn = datasource.getConnection()) {
      try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString(3);

          // Fixed: There's a tableName() helper used to create table names and also replaces stuff
          // in the original table name. Using tablePrefix directly is incorrect since it may pose
          // problems because stuff replacement has not been done in tablePrefix, so it may never
          // find the table to drop.
          if (startsWithIgnoreCase(tableName, tableNameModifier(tablePrefix))) {
            // only add if not the table holding store types
            String storeName = removeStart(tableName, tableNameModifier(tablePrefix));
            if (!STORE_TYPES_NAME.equalsIgnoreCase(storeName)) {
              names.add(storeName);
            }
          }
        }
      }
      return names;
    } catch (SQLException e) {
      throw new DataStoreException("Could not get store names.", e);
    }
  }

  @Override
  public Optional<Class<?>> getStoreType(String storeName) {
    if (storeName == null) {
      return Optional.empty();
    }
    Optional<String> typeStr = storeTypes.find(storeName);
    if (typeStr.isPresent()) {
      try {
        return Optional.ofNullable(ClassUtils.getClass(typeStr.get()));
      } catch (ClassNotFoundException e) {
        throw new DataStoreException("Could not determine type of: " + storeName, e);
      }
    }
    return Optional.empty();
  }

  @Override
  public void loadFromXML(XML xml) {
    List<XML> nodes = xml.getXMLList("datasource/property");
    for (XML node : nodes) {
      String name = node.getString("@name");
      String value = node.getString(".");
      configProperties.add(name, value);
    }
    setTablePrefix(xml.getString("tablePrefix", getTablePrefix()));
    setVarcharType(xml.getString("dataTypes/varchar/@use", getVarcharType()));
    setTimestapType(xml.getString("dataTypes/timestamp/@use", getTimestapType()));
    setTextType(xml.getString("dataTypes/text/@use", getTextType()));
  }

  @Override
  public void saveToXML(XML xml) {
    XML xmlDatasource = xml.addElement("datasource");
    for (Entry<String, List<String>> entry : configProperties.entrySet()) {
      List<String> values = entry.getValue();
      for (String value : values) {
        if (value != null) {
          xmlDatasource.addElement("property", value).setAttribute("name", entry.getKey());
        }
      }
    }
    xml.addElement("tablePrefix", getTablePrefix());
    XML dtXML = xml.addElement("dataTypes");
    dtXML.addElement("varchar").setAttribute("use", getVarcharType());
    dtXML.addElement("timestamp").setAttribute("use", getTimestapType());
    dtXML.addElement("text").setAttribute("use", getTextType());
  }

  ProsearchTableAdapter getTableAdapter() {
    return tableAdapter;
  }

  Connection getConnection() {
    try {
      return datasource.getConnection();
    } catch (SQLException e) {
      throw new DataStoreException("Could not get database connection.", e);
    }
  }

  String tableName(String storeName) {
    String n = tablePrefix + storeName.trim();
    return tableNameModifier(n);
  }

  String tableNameModifier(String name) {
    String modified = "" + name;
    modified = modified.replaceFirst("(?i)^[^a-z]", "x");
    modified = modified.replaceAll("\\W+", "_");

    return modified;
  }

  boolean tableExist(String tableName) {
    try (Connection conn = datasource.getConnection()) {
      try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          if (rs.getString(3).equalsIgnoreCase(tableName)) {
            return true;
          }
        }
      }
      return false;
    } catch (SQLException e) {
      throw new DataStoreException("Could not check if table '" + tableName + "' exists.", e);
    }
  }

  public Stream<HostCount> queuedEntries() throws SQLException {
    return Manager.queryDb(
        this.datasource,
        """
        SELECT DISTINCT host, 0 as c
        FROM %s
        GROUP BY host
        """
            .formatted(this.queuedTableName()),
        rs -> new HostCount(new Host(rs.getString(1)), rs.getInt(2)));
  }

  private String queuedTableName() {
    return this.tableName(JdbcStore.QUEUED_STORE);
  }

  public boolean isCacheEmpty() {
    if (this.tableExist(cachedTableName())) {

      try (var con = this.datasource.getConnection();
          var ps = con.prepareStatement("SELECT * FROM %s LIMIT 1".formatted(cachedTableName()))) {
        var rs = ps.executeQuery();

        return !rs.next();
      } catch (SQLException e) {
        throw new DataStoreException("Could not check if cache is empty.", e);
      }
    } else {
      throw new DataStoreException("cache table does not exist.");
    }
  }

  public boolean isQueueEmptyForHost(final Host host) {
    if (this.tableExist(queuedTableName())) {

      try (var con = this.datasource.getConnection();
          var ps =
              con.prepareStatement(
                  "SELECT COUNT(*) FROM %s WHERE host = ?".formatted(queuedTableName()))) {
        ps.setString(1, host.toString());
        var rs = ps.executeQuery();

        while (rs.next()) {

          // start url has been dequeued in the imported pipeline, so it shouldn't exist in queue
          if (rs.getInt(1) == 0) {
            return true;
          }
        }
        return false;
      } catch (SQLException e) {
        throw new DataStoreException("Could not check if queue is empty.", e);
      }
    } else {
      throw new DataStoreException("queue table does not exist.");
    }
  }

  private String cachedTableName() {
    return this.tableName(JdbcStore.CACHED_STORE);
  }
}
