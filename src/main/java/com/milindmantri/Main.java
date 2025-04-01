package com.milindmantri;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args)
      throws SQLException, ExecutionException, InterruptedException, IOException {

    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    try (var dataSource = new HikariDataSource(new HikariConfig(dbProps().toProperties()));
        ScheduledExecutorService crawlerScheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor()) {

      Set<URI> startUrls;
      try (InputStream is = classloader.getResourceAsStream("start-urls");
          var isr = new InputStreamReader(is);
          var reader = new BufferedReader(isr)) {

        startUrls = reader.lines().map(URI::create).collect(Collectors.toSet());

      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      int delayBetweenRuns =
          Integer.parseInt(System.getProperty("delay-hours-between-crawls", "4"));

      final TantivyClient client =
          new TantivyClient(
              HttpClient.newBuilder()
                  .followRedirects(HttpClient.Redirect.NORMAL)
                  .version(HttpClient.Version.HTTP_1_1)
                  .build(),
              URI.create(System.getProperty("tantivy-server", "http://localhost:3000")));

      SemaphoredExecutor exec =
          new SemaphoredExecutor(virtualThreads, Runtime.getRuntime().availableProcessors() * 2);
      Future<?> crawlerFuture =
          crawlerScheduler.scheduleWithFixedDelay(
              new CrawlerRunner(dataSource, client, startUrls.stream(), exec),
              0,
              delayBetweenRuns,
              TimeUnit.HOURS);

      int httpPort = Integer.parseInt(System.getProperty("http-server-port", "80"));

      HttpServer httpServer =
          httpServer(httpPort, client, dataSource, startUrls.stream(), virtualThreads);

      httpServer.start();

      try {
        crawlerFuture.get();
      } catch (Exception e) {
        LOGGER.error("Crawler failed to run.", e);
      }
    }
  }

  static HttpServer httpServer(
      final int port,
      final TantivyClient tantivyClient,
      final DataSource datasource,
      final Stream<URI> startUrls,
      final ExecutorService virtualThreads)
      throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    httpServer.setExecutor(virtualThreads);

    httpServer.createContext("/search/", new SearchHttpHandler(tantivyClient));
    httpServer.createContext(
        StatisticsHttpHandler.STATISTICS_PAGE_PATH,
        new StatisticsHttpHandler(datasource, startUrls));
    return httpServer;
  }

  static com.norconex.commons.lang.map.Properties dbProps() {

    final String dbName = System.getProperty("db-name");
    final String user = System.getProperty("db-user");
    final String pass = System.getProperty("db-pass");

    if (dbName == null || user == null || pass == null) {
      throw new IllegalArgumentException(
          "Properties 'db-name', 'db-user', 'db-pass' must be set and not null.");
    }

    var props = new HashMap<String, List<String>>();
    props.put("jdbcUrl", List.of("jdbc:postgresql://localhost:5432/" + dbName));
    props.put("username", List.of(user));
    props.put("password", List.of(pass));

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
