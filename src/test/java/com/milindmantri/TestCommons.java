package com.milindmantri;

import com.norconex.commons.lang.map.Properties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.poi.ss.formula.functions.T;

public class TestCommons {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  public static HikariDataSource createTestDataSource() {
    return new HikariDataSource(new HikariConfig(TestCommons.dbProps().toProperties()));
  }

  static com.norconex.commons.lang.map.Properties dbProps() {
    var props = new Properties();
    props.add("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
    props.add("username", "postgres");
    props.add("password", "pass");

    return props;
  }

  static void exec(final DataSource datasource, final String sql) throws SQLException {
    try (var con = datasource.getConnection();
        var s = con.createStatement(); ) {
      s.executeUpdate(sql);
    }
  }

  static <T> T query(final DataSource datasource, final String sql, Function<ResultSet, T> mapper)
      throws SQLException {
    try (var con = datasource.getConnection();
        var s = con.createStatement(); ) {
      return mapper.apply(s.executeQuery(sql));
    }
  }
}
