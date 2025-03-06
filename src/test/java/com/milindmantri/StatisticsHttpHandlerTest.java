package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StatisticsHttpHandlerTest {

  private static final HikariDataSource datasource = TestCommons.createTestDataSource();

  @AfterAll
  static void close() throws SQLException {
    new Manager(1, datasource).dropStatsTable();
    datasource.close();
  }

  @BeforeAll
  static void create() throws SQLException {
    new Manager(1, datasource).createStatsTableIfNotExists();
  }

  @BeforeEach
  void clear() throws SQLException {
    try (var con = datasource.getConnection();
        var stmt = con.createStatement()) {
      stmt.executeUpdate("TRUNCATE domain_stats");
    }
  }

  @Test
  void statsOrder() throws SQLException {
    final String INSERT_STMT =
        """
  INSERT INTO domain_stats (host, url, length)
  VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?);
  """;

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(INSERT_STMT);
        var getStats = con.prepareStatement(StatisticsHttpHandler.GET_STATS_QUERY)) {

      ps.setString(1, "www.example.com");
      ps.setString(2, "http://www.example.com/path");
      ps.setLong(3, 50);

      ps.setString(4, "www.example.com");
      ps.setString(5, "http://www.example.com/path2");
      ps.setLong(6, 50);

      ps.setString(7, "www.hello.com");
      ps.setString(8, "http://www.hello.com/path2");
      ps.setLong(9, 50);
      ps.executeUpdate();

      var stats =
          new StatisticsHttpHandler(
              datasource,
              Stream.of(
                      "http://www.uncrawlable.com",
                      "http://www.hello.com",
                      "http://www.example.com")
                  .map(URI::create));

      var statStream = stats.getSummary(getStats.executeQuery());

      var list = statStream.map(StatisticsHttpHandler.Stat::domain).toList();
      assertEquals(
          Stream.of("www.example.com", "www.hello.com", "www.uncrawlable.com")
              .map(Host::new)
              .toList(),
          list);
    }
  }
}
