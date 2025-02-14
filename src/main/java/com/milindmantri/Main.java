package com.milindmantri;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

public class Main {

  private static final String CREATE_DOMAIN_STATS_TABLE =
      """
      CREATE TABLE IF NOT EXISTS
        domain_stats (
            host   VARCHAR NOT NULL
          , url    VARCHAR NOT NULL
          , length bigint  NOT NULL
        );
      """;

  private static final String CREATE_DOMAIN_STATS_INDEX =
      """
      CREATE UNIQUE INDEX IF NOT EXISTS
        domain_stats_idx
      ON domain_stats (
          host
        , url
      )
      """;

  public static void main(String[] args)
      throws SQLException, ExecutionException, InterruptedException, IOException {

    try (var dataSource = new HikariDataSource(new HikariConfig(dbProps().toProperties()));
        ScheduledExecutorService crawlerScheduler = Executors.newSingleThreadScheduledExecutor()) {

      // TODO: Remove once testing is complete
      dropStatsTable(dataSource);

      createStatsTableIfNotExists(dataSource);

      int delayBetweenRuns =
          Integer.parseInt(System.getProperty("delay-hours-between-crawls", "4"));

      final TantivyClient client =
          new TantivyClient(
              HttpClient.newBuilder()
                  .followRedirects(HttpClient.Redirect.NORMAL)
                  .version(HttpClient.Version.HTTP_1_1)
                  .build(),
              URI.create(System.getProperty("tantivy-server", "http://localhost:3000")));

      crawlerScheduler.scheduleWithFixedDelay(
          new CrawlerRunner(dataSource, client), 0, delayBetweenRuns, TimeUnit.HOURS);

      int httpPort = Integer.parseInt(System.getProperty("http-server-port", "80"));

      HttpServer httpServer = httpServer(httpPort, client, dataSource);

      httpServer.start();

      Thread.currentThread().join();
    }
  }

  static HttpServer httpServer(
      final int port, final TantivyClient tantivyClient, final DataSource datasource)
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

    httpServer.createContext("/search/", new SearchHttpHandler(tantivyClient));
    httpServer.createContext(
        StatisticsHttpHandler.STATISTICS_PAGE_PATH, new StatisticsHttpHandler(datasource));
    return httpServer;
  }

  static void createStatsTableIfNotExists(final DataSource datasource) throws SQLException {
    try (var con = datasource.getConnection();
        var createTable = con.prepareStatement(CREATE_DOMAIN_STATS_TABLE);
        var createIndex = con.prepareStatement(CREATE_DOMAIN_STATS_INDEX)) {

      con.setAutoCommit(false);
      createTable.executeUpdate();
      createIndex.executeUpdate();

      con.commit();
      con.setAutoCommit(true);
    }
  }

  private static void dropStatsTable(final DataSource datasource) throws SQLException {
    try (var con = datasource.getConnection()) {
      con.createStatement().executeUpdate("DROP TABLE IF EXISTS domain_stats");
    }
  }

  static com.norconex.commons.lang.map.Properties dbProps() {
    var props = new HashMap<String, List<String>>();
    props.put("jdbcUrl", List.of("jdbc:postgresql://localhost:5432/milind"));
    props.put("username", List.of("postgres"));
    props.put("password", List.of("pass"));

    return new com.norconex.commons.lang.map.Properties(props);
  }

  public static void write(final Writer writer, final String str) {
    try {
      writer.write(str);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
