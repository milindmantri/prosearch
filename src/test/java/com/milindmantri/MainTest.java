package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
