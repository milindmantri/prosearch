package com.milindmantri;

import com.milindmantri.pages.SearchPage;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static final String QUERY_PARAM = "q";

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

      HttpServer httpServer = httpServer(httpPort, client);

      httpServer.start();
    }
  }

  static HttpServer httpServer(final int port, final TantivyClient tantivyClient)
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

    httpServer.createContext(
        "/search/",
        exchange -> {
          final URI uri = exchange.getRequestURI();
          final List<NameValuePair> nvp =
              URLEncodedUtils.parse(uri.getRawQuery(), StandardCharsets.UTF_8);

          final var maybeSearchTerm =
              nvp.stream()
                  .filter(n -> QUERY_PARAM.equals(n.getName()))
                  .map(NameValuePair::getValue)
                  .filter(str -> !str.isBlank())
                  .findFirst();

          exchange.getResponseHeaders().add("Content-Type", "text/html");

          try {
            if (maybeSearchTerm.isPresent()) {
              final String term = maybeSearchTerm.get();

              var searchResult =
                  tantivyClient.search(URLEncoder.encode(term, StandardCharsets.UTF_8));

              var sp = new SearchPage(term, searchResult);

              // need to send response headers before calling getResponseBody (API limitation)
              exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);

              try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
                  var writer = new BufferedWriter(outWriter)) {

                sp.html().map(Html::toHtml).forEach(str -> Main.write(writer, str));
              }

            } else {
              // empty search page
              exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
              try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
                  var writer = new BufferedWriter(outWriter)) {

                new SearchPage().html().map(Html::toHtml).forEach(str -> Main.write(writer, str));
              }
            }

          } catch (Exception e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
            try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
                var writer = new BufferedWriter(outWriter)) {
              LOGGER.error("Failed to process request.", e);
              writer.write("Internal server error. Contact: milind -at- milindmantri -dot- com");
            }
          } finally {
            exchange.close();
          }
        });
    return httpServer;
  }

  private static void write(final Writer writer, final String str) {
    try {
      writer.write(str);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
}
