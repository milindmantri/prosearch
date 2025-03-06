package com.milindmantri;

import com.norconex.collector.core.crawler.CrawlerEvent;
import com.norconex.collector.core.doc.CrawlState;
import com.norconex.collector.core.filter.IMetadataFilter;
import com.norconex.collector.core.filter.IReferenceFilter;
import com.norconex.collector.core.filter.impl.MetadataFilter;
import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.norconex.collector.http.delay.IDelayResolver;
import com.norconex.collector.http.delay.impl.AbstractDelayResolver;
import com.norconex.collector.http.delay.impl.GenericDelayResolver;
import com.norconex.collector.http.fetch.HttpMethod;
import com.norconex.collector.http.fetch.IHttpFetcher;
import com.norconex.collector.http.fetch.impl.GenericHttpFetcher;
import com.norconex.collector.http.robot.RobotsTxt;
import com.norconex.commons.lang.event.Event;
import com.norconex.commons.lang.event.IEventListener;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import com.norconex.commons.lang.text.TextMatcher;
import com.norconex.importer.doc.Doc;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Manager
    implements IMetadataFilter,
        IEventListener<Event>,
        IReferenceFilter,
        IPipelineStage<ImporterPipelineContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Manager.class);

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

  private final int limit;

  private final Map<Host, AtomicInteger> count = new ConcurrentHashMap<>();

  private final DataSource dataSource;
  // TODO: make final
  private final Set<Host> notQueuedHosts = ConcurrentHashMap.newKeySet();
  private Host[] startUrls;
  private PrimitiveIterator.OfInt nextHostIndex;

  // TODO: Add custom first stage which rejects in importer pipeline and remove delay resolver
  private final GenericDelayResolver delayResolver =
      new GenericDelayResolver() {
        @Override
        public void delay(final RobotsTxt robotsTxt, final String url) {
          if (acceptReference(url)) {
            super.delay(robotsTxt, url);
          }
        }

        @Override
        protected long resolveExplicitDelay(final String url) {
          if (acceptReference(url)) {
            return Integer.parseInt(System.getProperty("crawl-download-delay-seconds", "1"))
                * 1000L;
          } else {
            return 0;
          }
        }
      };

  private final GenericHttpFetcher modifiedHttpFetcher =
      new GenericHttpFetcher() {
        @Override
        public boolean accept(final Doc doc, final HttpMethod httpMethod) {
          // Done because already queued docs don't go through ref filter again
          // And if we have already hit the limit, why fetch queued docs
          if (httpMethod == HttpMethod.HEAD && doc.getReference() != null) {
            if (acceptReference(doc.getReference())) {
              return super.accept(doc, httpMethod);
            } else {
              LOGGER.info(
                  "Rejecting from HTTP fetcher since limit reached for ref {}", doc.getReference());
              return false;
            }
          } else {

            return super.accept(doc, httpMethod);
          }
        }
      };

  /** Caller is responsible for closing dataSource */
  @Deprecated(forRemoval = true)
  public Manager(final int limit, final DataSource dataSource) {
    if (limit <= 0) {
      throw new IllegalArgumentException(
          "Limit must be greater than zero, but was %d.".formatted(limit));
    }

    if (dataSource == null) {
      throw new IllegalArgumentException("{dataSource} must not be null.");
    }

    this.limit = limit;
    this.dataSource = dataSource;

    this.delayResolver.setScope(AbstractDelayResolver.SCOPE_SITE);
  }

  /** Caller is responsible for closing dataSource */
  public Manager(final int limit, final DataSource dataSource, final Stream<URI> startUrls)
      throws SQLException {
    this(limit, dataSource);

    this.startUrls = startUrls.map(Host::new).toArray(Host[]::new);
    // infinite stream over index
    this.nextHostIndex = IntStream.iterate(0, i -> (i + 1) % this.startUrls.length).iterator();
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
    final Host host = new Host(uri);
    final String reference = removeScheme(uri);

    // TODO: insertIntoDb and local count handling should happen in a txn

    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() >= limit) {
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
    } else if (event instanceof CrawlerEvent ce && ce.is(CrawlerEvent.DOCUMENT_QUEUED)) {
      // host has been queued, remove from not queued hosts if it exists there.
      Host host = new Host(URI.create(ce.getCrawlDocInfo().getReference()));

      if (!isQueuedOnce(host)) {
        count.put(host, new AtomicInteger(0));
      }
      this.notQueuedHosts.remove(host);
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
    final Host host = new Host(URI.create(normalized));
    return acceptHost(host);
  }

  public IHttpFetcher httpFetcher() {
    return this.modifiedHttpFetcher;
  }

  @Override
  public boolean execute(final ImporterPipelineContext context) {
    // results in NPE if not set (incorrect expectation in lib)
    context.getDocInfo().setState(CrawlState.REJECTED);

    return acceptHost(new Host(URI.create(context.getDocument().getReference())));
  }

  public void restoreCount(final JdbcStoreEngine engine) throws SQLException {

    if (engine.hasQueuedTable()) {
      restoreInternal(
          """
        SELECT DISTINCT host, 0 as c
        FROM %s
        GROUP BY host
        """
              .formatted(engine.queuedTableName()));
    }

    restoreInternal(
        """
    SELECT
      host, count(*)
    FROM
      host_count
    GROUP BY host
    """);
  }

  static class WrappedSqlException extends RuntimeException {
    public WrappedSqlException(final Exception e) {
      super(e);
    }
  }

  void createStatsTableIfNotExists() throws SQLException {
    try (var con = this.dataSource.getConnection();
        var createTable = con.prepareStatement(CREATE_DOMAIN_STATS_TABLE);
        var createIndex = con.prepareStatement(CREATE_DOMAIN_STATS_INDEX)) {

      con.setAutoCommit(false);
      createTable.executeUpdate();
      createIndex.executeUpdate();

      con.commit();
      con.setAutoCommit(true);
    }
  }

  void dropStatsTable() throws SQLException {
    try (var con = this.dataSource.getConnection()) {
      con.createStatement().executeUpdate("DROP TABLE IF EXISTS domain_stats");
    }
  }

  void restoreInternal(final String sql) throws SQLException {
    try (var con = this.dataSource.getConnection();
        var ps = con.prepareStatement(sql)) {

      ResultSet rs = ps.executeQuery();

      try {
        Stream.iterate(
                rs,
                resultSet -> {
                  try {
                    return resultSet.next();
                  } catch (SQLException e) {
                    throw new WrappedSqlException(e);
                  }
                },
                resultSet -> resultSet)
            .forEach(
                resultSet -> {
                  try {
                    this.count.put(
                        new Host(resultSet.getString(1)), new AtomicInteger(resultSet.getInt(2)));
                  } catch (SQLException e) {
                    throw new WrappedSqlException(e);
                  }
                });
      } catch (WrappedSqlException e) {
        throw ((SQLException) e.getCause());
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

  private boolean insertIntoDb(final Host host, final String reference) {
    try (var con = this.dataSource.getConnection();
        var ps = con.prepareStatement("INSERT INTO host_count(host, url) VALUES (?, ?)")) {

      ps.setString(1, host.toString());
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

  public IDelayResolver delayResolver() {
    return this.delayResolver;
  }

  public static String removeScheme(final URI uri) {
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

  public boolean isQueuedOnce(Host host) {
    return this.count.containsKey(host);
  }

  public boolean acceptHost(Host host) {
    if (isQueuedOnce(host)) {
      AtomicInteger i = count.get(host);

      return i.get() < limit;
    } else {
      return true;
    }
  }

  /** Helper to get next host to pull entry from queue */
  public Optional<Host> getNextHost() {

    if (this.nextHostIndex.hasNext()) {

      Host host = null;
      int firstIndex = -1;

      do {
        int i = this.nextHostIndex.nextInt();

        if (host == null) {
          firstIndex = i;
        } else if (i == firstIndex) {
          // all urls exhausted
          return Optional.empty();
        }

        host = this.startUrls[i];
      } while (!(isQueuedOnce(host) && acceptHost(host) && maybeQueued(host))
          && this.nextHostIndex.hasNext());

      return Optional.of(host);
    }
    return Optional.empty();
  }

  private boolean maybeQueued(final Host host) {
    return !this.notQueuedHosts.contains(host);
  }

  /** Call when host is not found when looking in queue */
  public void notQueued(final Host host) {
    this.notQueuedHosts.add(host);
  }

  void insertIntoDomainStats(final URI uri, final long length) throws SQLException {
    try (var con = this.dataSource.getConnection();
        var ps =
            con.prepareStatement("INSERT INTO domain_stats (host, url, length) VALUES (?, ?, ?)")) {
      ps.setString(1, Objects.requireNonNull(uri.getAuthority()));

      ps.setString(2, Manager.removeScheme(uri));
      ps.setLong(3, length);
      ps.executeUpdate();
    }
  }

  void deleteFromDomainStats(final URI uri) throws SQLException {
    try (var con = this.dataSource.getConnection();
        var ps = con.prepareStatement("DELETE FROM domain_stats WHERE host = ? AND url = ?")) {
      ps.setString(1, Objects.requireNonNull(uri.getAuthority()));

      ps.setString(2, Manager.removeScheme(uri));
      ps.executeUpdate();
    }
  }
}
