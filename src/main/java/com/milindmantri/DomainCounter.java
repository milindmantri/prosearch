package com.milindmantri;

import com.norconex.collector.core.crawler.CrawlerEvent;
import com.norconex.collector.core.filter.IReferenceFilter;
import com.norconex.commons.lang.event.Event;
import com.norconex.commons.lang.event.IEventListener;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DomainCounter implements IReferenceFilter, IEventListener<Event> {

  private static final String CREATE_TABLE =
      """
  CREATE TABLE IF NOT EXISTS
    host_count
  (
      host VARCHAR
    , url VARCHAR
  )
  """;

  private final int limit;

  private final Map<String, AtomicInteger> count = new ConcurrentHashMap<>();

  private final Properties dbProps;

  public DomainCounter(final int limit, final Properties props) throws SQLException {
    if (limit <= 0) {
      throw new IllegalArgumentException(
          "Limit must be greater than zero, but was %d.".formatted(limit));
    }

    if (props == null) {
      throw new IllegalArgumentException("{props} must not be null.");
    }

    this.limit = limit;
    this.dbProps = props;

    restoreCount(props);
  }

  @Override
  public boolean acceptReference(final String reference) {
    String host = URI.create(reference).getHost();

    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() == limit) {
        return false;
      } else {
        return i.incrementAndGet() <= limit;
      }

    } else {
      count.put(host, new AtomicInteger(1));
      return true;
    }
  }

  @Override
  public void accept(final Event event) {

    if (event.is(CrawlerEvent.CRAWLER_INIT_BEGIN)) {
      // create table

      try (var datasource = new HikariDataSource(new HikariConfig(dbProps));
          var con = datasource.getConnection();
          var ps = con.prepareStatement(CREATE_TABLE)) {

        ps.executeUpdate();

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  void restoreCount(final Properties props) throws SQLException {

    try (var datasource = new HikariDataSource(new HikariConfig(props));
        var con = datasource.getConnection();
        var ps =
            con.prepareStatement(
                """
            SELECT
              count(*), host
            FROM
              host_count
            GROUP BY host
          """)) {

      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        int count = rs.getInt(1);
        String host = rs.getString(2);

        this.count.put(host, new AtomicInteger(count));
      }
    }
  }
}
