package com.milindmantri;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
      throws SQLException, ExecutionException, InterruptedException {

    try (var dataSource = new HikariDataSource(new HikariConfig(dbProps().toProperties()));
        ScheduledExecutorService crawlerScheduler = Executors.newSingleThreadScheduledExecutor()) {

      // TODO: Remove once testing is complete
      dropStatsTable(dataSource);

      createStatsTableIfNotExists(dataSource);

      int delayBetweenRuns =
          Integer.parseInt(System.getProperty("delay-hours-between-crawls", "4"));
      ScheduledFuture<?> sf =
          crawlerScheduler.scheduleWithFixedDelay(
              new CrawlerRunner(dataSource), 0, delayBetweenRuns, TimeUnit.HOURS);

      sf.get();
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
