package com.milindmantri;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Properties;

public class TestCommons {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  public static HikariDataSource createTestDataSource() {
    return new HikariDataSource(new HikariConfig(TestCommons.dbProps()));
  }

  private static Properties dbProps() {
    var props = new Properties();
    props.put("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
    props.put("username", "postgres");
    props.put("password", "pass");

    return props;
  }
}
