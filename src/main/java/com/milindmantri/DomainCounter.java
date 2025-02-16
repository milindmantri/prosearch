package com.milindmantri;

import com.norconex.collector.core.crawler.CrawlerEvent;
import com.norconex.collector.core.filter.IMetadataFilter;
import com.norconex.collector.core.filter.IReferenceFilter;
import com.norconex.collector.core.filter.impl.MetadataFilter;
import com.norconex.commons.lang.event.Event;
import com.norconex.commons.lang.event.IEventListener;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.text.TextMatcher;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DomainCounter implements IMetadataFilter, IEventListener<Event>, IReferenceFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DomainCounter.class);

  private static final List<IMetadataFilter> CONTENT_TYPE_FILTERS = getTextOnlyMetadataFilters();

  // Visible to tests
  static final String CREATE_TABLE =
      """
      CREATE TABLE IF NOT EXISTS
        host_count
      (
          host VARCHAR NOT NULL
        , url  VARCHAR NOT NULL
      )
      """;

  static final String CREATE_INDEX =
      """
      CREATE UNIQUE INDEX IF NOT EXISTS
        host_count_index
      ON
        host_count (host, url)
      """;

  private static final String PG_UNDEFINED_RELATION_ERR_CODE = "42P01";
  private static final String PG_UNIQUE_VIOLATION_ERR_CODE = "23505";

  private final int limit;

  private final Map<String, AtomicInteger> count = new ConcurrentHashMap<>();

  private final DataSource dataSource;

  /** Caller is responsible for closing dataSource */
  public DomainCounter(final int limit, final DataSource dataSource) throws SQLException {
    if (limit <= 0) {
      throw new IllegalArgumentException(
          "Limit must be greater than zero, but was %d.".formatted(limit));
    }

    if (dataSource == null) {
      throw new IllegalArgumentException("{dataSource} must not be null.");
    }

    this.limit = limit;
    this.dataSource = dataSource;

    restoreCount();
  }

  @Override
  public boolean acceptMetadata(final String ref, final Properties metadata) {
    // Only if content-type matches expectation, we want to play with host_count

    final boolean isContentValid =
        CONTENT_TYPE_FILTERS.stream().anyMatch(f -> f.acceptMetadata(ref, metadata));

    if (!isContentValid) {
      LOGGER.info("Unacceptable content for ref {}", ref);
      return false;
    }

    // ref is already normalized
    final URI uri = URI.create(ref);
    final String host = uri.getRawAuthority();
    final String reference = removeScheme(uri);

    // TODO: insertIntoDb and local count handling should happen in a txn

    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() == limit) {
        LOGGER.info("Filtered by metadata: host {}, ref {}", host, reference);
        return false;
      } else {
        return insertIntoDb(host, reference) && i.incrementAndGet() <= limit;
      }

    } else {
      if (insertIntoDb(host, reference)) {
        count.put(host, new AtomicInteger(1));
        return true;
      } else {
        throw new IllegalStateException("Inserting host in DB failed, without being initialized.");
      }
    }
  }

  @Override
  public void accept(final Event event) {
    // TODO: When non-canonical links are processed (previously enqueued) with the host count
    // limit reached, canonical refs are rejected by DomainCounter which is a problem since,
    // we want to follow canonical link and index it instead of the enqueued non-canonical.

    // Unfortunately, only a REJECTED_NONCANONICAL is thrown. We need something like a
    // FOUND_CANONICAL to process correctly even when the host count limit is reached. Until then,
    // ignoring canonical links (CrawlerConfig).

    if (event.is(CrawlerEvent.CRAWLER_INIT_BEGIN)) {
      // create table

      try (var con = this.dataSource.getConnection();
          var createTablePs = con.prepareStatement(CREATE_TABLE);
          var indexStmt = con.createStatement()) {

        createTablePs.executeUpdate();
        indexStmt.executeUpdate(CREATE_INDEX);

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (event.is(CrawlerEvent.CRAWLER_RUN_END) || event.is(CrawlerEvent.CRAWLER_CLEAN_END)) {
      try (var con = this.dataSource.getConnection();
          // TODO: IF EXISTS?
          var createTablePs = con.prepareStatement("DROP TABLE host_count")) {

        createTablePs.executeUpdate();

        count.clear();

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // This is a faster track to reject URLs when limit is reached instead of doing a HEAD request
  // and rejecting then in the metadata filter.
  // When true, it is left to the metadata filter to accept or not.
  @Override
  public boolean acceptReference(final String reference) {

    // since acceptMetadata will get a normalized URL only
    final String normalized = CrawlerRunner.URL_NORMALIZER.normalizeURL(reference);

    // ref is already normalized
    final URI uri = URI.create(normalized);
    final String host = uri.getRawAuthority();
    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() == limit) {
        LOGGER.info("Filtered by reference: host {}, ref {}", host, reference);
        return false;
      } else {
        return true;
      }
    } else {
      return true;
    }
  }

  private void restoreCount() throws SQLException {

    try (var con = this.dataSource.getConnection();
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
    } catch (final SQLException ex) {
      // check if table exists
      if (!PG_UNDEFINED_RELATION_ERR_CODE.equals(ex.getSQLState())) {
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        // table exists, but fetch failed. Don't throw if table doesn't exist.

        throw ex;
      }
    }
  }

  private boolean insertIntoDb(final String host, final String reference) {
    try (var con = this.dataSource.getConnection();
        var ps = con.prepareStatement("INSERT INTO host_count(host, url) VALUES (?, ?)")) {

      ps.setString(1, host);
      ps.setString(2, reference);

      ps.executeUpdate();
      return true;
    } catch (SQLException e) {
      if (PG_UNIQUE_VIOLATION_ERR_CODE.equals(e.getSQLState())) {
        LOGGER.info("Rejecting duplicate: host {} ref {}", host, reference);
        return false;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private static String removeScheme(final URI uri) {
    var sb = new StringBuilder();
    sb.append(uri.getRawAuthority());
    if (uri.getRawPath() != null) {
      sb.append(uri.getRawPath());
    }

    if (uri.getRawQuery() != null) {
      sb.append('?');
      sb.append(uri.getRawQuery());
    }

    return sb.toString();
  }

  private static List<IMetadataFilter> getTextOnlyMetadataFilters() {
    return Stream.of("text/html", "application/xhtml+xml", "text/plain")
        .map(t -> t + "*")
        .<IMetadataFilter>mapMulti(
            (t, c) -> {
              c.accept(
                  new MetadataFilter(TextMatcher.basic("Content-Type"), TextMatcher.wildcard(t)));
              c.accept(
                  new MetadataFilter(TextMatcher.basic("content-type"), TextMatcher.wildcard(t)));
            })
        .toList();
  }
}
