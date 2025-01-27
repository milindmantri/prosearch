package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DomainCounterTest {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  private static final DataSource datasource = new HikariDataSource(new HikariConfig(dbProps()));

  private static final String CREATE_TABLE =
      """
    CREATE TABLE IF NOT EXISTS
      host_count
    (
        host VARCHAR
      , url VARCHAR
    )
    """;

  private static final String DROP_TABLE =
      """
    DROP TABLE IF EXISTS host_count;
    """;

  @BeforeEach
  void createTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(CREATE_TABLE)) {
      ps.executeUpdate();
    }
  }

  @AfterEach
  void dropTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(DROP_TABLE)) {
      ps.executeUpdate();
    }
  }

  @Test
  void invalidLimit() {
    assertThrows(IllegalArgumentException.class, () -> new DomainCounter(-1, dbProps()));
    assertThrows(IllegalArgumentException.class, () -> new DomainCounter(-1, null));
  }

  @Test
  void stopAfterLimitSingleHost() throws SQLException {
    // should stop after limit is reached
    DomainCounter dc = new DomainCounter(3, dbProps());

    assertTrue(
        IntStream.range(0, 3)
            .mapToObj(i -> dc.acceptReference("http://host.com/%d".formatted(i)))
            .allMatch(b -> b));

    assertFalse(dc.acceptReference("http://host.com/4"));
  }

  @Test
  void restoreCountWhenStarting() throws SQLException {

    try (var con = datasource.getConnection();
        var ps =
            con.prepareStatement(
                """
            INSERT INTO
              host_count
            VALUES
                ('host.com', 'http://host.com/1')
              , ('host.com', 'http://host.com/2')
            """)) {
      ps.executeUpdate();
    }

    DomainCounter dc = new DomainCounter(3, dbProps());

    assertTrue(dc.acceptReference("http://host.com/3"));

    assertFalse(dc.acceptReference("http://host.com/4"));
  }

  // TODO: increment in DB test
  // TODO: add index
  // TODO: add sql
  // TODO: add clearing mechanism

  private static Properties dbProps() {
    var props = new Properties();
    props.put("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
    props.put("username", "postgres");
    props.put("password", "pass");

    return props;
  }
}
