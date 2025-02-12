package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MainTest {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  private static final HikariDataSource datasource =
      new HikariDataSource(new HikariConfig(dbProps()));

  @AfterAll
  static void closeDataSource() {
    datasource.close();
  }

  @AfterEach
  @BeforeEach
  void dropTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("DROP TABLE IF EXISTS domain_stats")) {
      ps.executeUpdate();
    }
  }

  @Test
  void server() throws IOException, InterruptedException, TantivyClient.FailedSearchException {
    TantivyClient tantivy = Mockito.mock(TantivyClient.class);
    Mockito.when(tantivy.search("hello")).thenReturn(Stream.of("content"));

    HttpServer httpServer = Main.httpServer(0, tantivy);
    httpServer.start();
    HttpClient client = HttpClient.newHttpClient();
    HttpResponse<String> res =
        client.send(
            HttpRequest.newBuilder()
                .GET()
                .uri(
                    URI.create(
                        "http://localhost:%s/search/hello"
                            .formatted(httpServer.getAddress().getPort())))
                .build(),
            HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpURLConnection.HTTP_OK, res.statusCode());
    assertEquals("content", res.body());

    httpServer.stop(0);
  }

  @Test
  void createStatsTableIfNotExists() throws SQLException {
    Main.createStatsTableIfNotExists(datasource);

    String host = "http://www.example.com";
    String url = "http://www.example.com/path";
    long length = 50_000;

    final String INSERT_STMT =
        """
    INSERT INTO domain_stats (host, url, length)
    VALUES (?, ?, ?);
    """;

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(INSERT_STMT)) {

      ps.setString(1, host);
      ps.setString(2, url);
      ps.setLong(3, length);

      assertDoesNotThrow(() -> ps.executeUpdate());
    }
  }

  private static Properties dbProps() {
    var props = new Properties();
    props.put("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
    props.put("username", "postgres");
    props.put("password", "pass");

    return props;
  }
}
