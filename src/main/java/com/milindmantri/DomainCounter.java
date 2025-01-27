package com.milindmantri;

import com.norconex.collector.core.filter.IReferenceFilter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DomainCounter implements IReferenceFilter {

  private final int limit;

  private final Map<String, AtomicInteger> count = new ConcurrentHashMap<>();

  public DomainCounter(final int limit, final Properties props) throws SQLException {
    if (limit <= 0) {
      throw new IllegalArgumentException(
          "Limit must be greater than zero, but was %d.".formatted(limit));
    }

    if (props == null) {
      throw new IllegalArgumentException("{props} must not be null.");
    }

    this.limit = limit;
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
