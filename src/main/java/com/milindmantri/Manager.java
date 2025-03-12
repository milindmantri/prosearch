package com.milindmantri;

import static com.norconex.collector.core.doc.CrawlDocMetadata.IS_CRAWL_NEW;

import com.norconex.collector.core.doc.CrawlState;
import com.norconex.collector.core.filter.IMetadataFilter;
import com.norconex.collector.core.filter.IReferenceFilter;
import com.norconex.collector.core.filter.impl.MetadataFilter;
import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.norconex.collector.http.delay.IDelayResolver;
import com.norconex.collector.http.delay.impl.AbstractDelayResolver;
import com.norconex.collector.http.delay.impl.GenericDelayResolver;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import com.norconex.commons.lang.text.TextMatcher;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.SequencedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Manager
    implements IMetadataFilter, IReferenceFilter, IPipelineStage<ImporterPipelineContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Manager.class);

  private static final List<IMetadataFilter> CONTENT_TYPE_FILTERS = getTextOnlyMetadataFilters();

  private static final String PG_UNIQUE_VIOLATION_ERR_CODE = "23505";
  static final String CREATE_DOMAIN_STATS_TABLE =
      """
    CREATE TABLE IF NOT EXISTS
      domain_stats (
          host   VARCHAR NOT NULL
        , url    VARCHAR NOT NULL
        , length bigint  NOT NULL
      );
    """;
  static final String CREATE_DOMAIN_STATS_INDEX =
      """
    CREATE UNIQUE INDEX IF NOT EXISTS
      domain_stats_idx
    ON domain_stats (
        host
      , url
    )
    """;

  private final int limit;

  private final ConcurrentMap<Host, AtomicInteger> count = new ConcurrentHashMap<>();

  private final DataSource dataSource;
  // TODO: make final
  private Host[] startUrls;
  private SequencedSet<Host> startUrlsSet;
  private PrimitiveIterator.OfInt nextHostIndex;

  private ProCrawler crawler;

  private final GenericDelayResolver delayResolver =
      new GenericDelayResolver() {
        @Override
        protected long resolveExplicitDelay(final String url) {
          return Integer.parseInt(System.getProperty("crawl-download-delay-seconds", "1")) * 1000L;
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

    this.startUrlsSet =
        startUrls.map(Host::new).collect(Collectors.toCollection(LinkedHashSet::new));
    this.startUrls = this.startUrlsSet.toArray(Host[]::new);
    // infinite stream over index
    this.nextHostIndex = IntStream.iterate(0, i -> (i + 1) % this.startUrls.length).iterator();
  }

  public void setCrawler(ProCrawler crwl) {
    this.crawler = crwl;
  }

  @Override
  public boolean acceptMetadata(final String ref, final Properties metadata) {
    final boolean isContentValid =
        CONTENT_TYPE_FILTERS.stream().anyMatch(f -> f.acceptMetadata(ref, metadata));

    if (!isContentValid) {
      LOGGER.info("Unacceptable content for ref {}", ref);
      return false;
    } else {
      return true;
    }
  }

  // TODO: When non-canonical links are processed (previously enqueued) with the host count
  // limit reached, canonical refs are rejected by DomainCounter which is a problem since,
  // we want to follow canonical link and index it instead of the enqueued non-canonical.

  // Unfortunately, only a REJECTED_NONCANONICAL is thrown. We need something like a
  // FOUND_CANONICAL to process correctly even when the host count limit is reached. Until then,
  // ignoring canonical links (CrawlerConfig).

  public boolean initCount(final Host host) {
    // host has been queued, remove from not queued hosts if it exists there.
    this.count.putIfAbsent(host, new AtomicInteger(0));
    return true;
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
    return isAcceptable(host);
  }

  boolean isRecrawling() {
    return this.crawler != null && this.crawler.isRecrawling();
  }

  private JdbcStoreEngine getEngine() {
    if (this.crawler != null) {
      return (JdbcStoreEngine) this.crawler.getDataStoreEngine();

    } else {
      throw new IllegalStateException("crawler not initialized.");
    }
  }

  @Override
  public boolean execute(final ImporterPipelineContext context) {
    // crawler lib never writes to cache store unless it is recrawling, where it renames processed
    // to cached and starts going over start urls and cached.

    URI uri = URI.create(context.getDocument().getReference());
    Host host = new Host(uri);

    // recrawling, as it is only set when doc is found in cache
    if (!context.getDocument().getMetadata().getBoolean(IS_CRAWL_NEW)) {
      return true;
    }

    boolean isAccepted = acceptHost(host);
    boolean hasStartUrlFormat =
        (uri.getPath() == null || uri.getPath().isEmpty() || "/".equals(uri.getPath()))
            && uri.getQuery() == null
            && uri.getFragment() == null;

    // when recrawling, and start urls could be http which may not be found. Need to allow links
    // equal or close to start url to enqueue the crawled links and begin the recrawl
    if (this.isRecrawling() && hasStartUrlFormat && this.startUrlsSet.contains(host)) {
      return this.getEngine().isQueueEmptyForHost(host);
    }
    if (isAccepted) {
      return true;
    } else {
      // results in NPE if not set (incorrect expectation in lib)
      context.getDocInfo().setState(CrawlState.REJECTED);

      return false;
    }
  }

  public void restoreCount() throws SQLException {
    var engine = (JdbcStoreEngine) this.crawler.getDataStoreEngine();

    Stream<HostCount> queued = engine.queuedEntries();

    Stream<HostCount> domainStats =
        queryDb(
            this.dataSource,
            """
            SELECT
              host, count(*)
            FROM
              domain_stats
            GROUP BY host
            """,
            rs -> new HostCount(new Host(rs.getString(1)), rs.getInt(2)));

    try (var stream = Stream.concat(queued, domainStats)) {
      stream.forEachOrdered(hc -> this.count.put(hc.host(), new AtomicInteger(hc.count())));
    }
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

  /** Caller needs to close the stream */
  public static <T> Stream<T> queryDb(
      final DataSource ds, final String sql, final ResultSetHandler<T> handler)
      throws SQLException {

    var con = ds.getConnection();
    var ps = con.prepareStatement(sql);

    ResultSet rs = ps.executeQuery();

    try {
      return Stream.iterate(
              rs,
              resultSet -> {
                try {
                  return resultSet.next();
                } catch (SQLException e) {
                  throw new WrappedSqlException(e);
                }
              },
              resultSet -> resultSet)
          .map(
              resultSet -> {
                try {
                  return handler.accept(resultSet);
                } catch (SQLException e) {
                  throw new WrappedSqlException(e);
                }
              })
          .onClose(
              () -> {
                try {
                  con.close();
                  ps.close();
                } catch (SQLException e) {
                  throw new WrappedSqlException(e);
                }
              });
    } catch (WrappedSqlException e) {
      throw ((SQLException) e.getCause());
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

  private boolean acceptHost(Host host) {

    if (isQueuedOnce(host)) {
      AtomicInteger i = count.get(host);

      return i.get() < limit;
    } else {
      return false;
    }
  }

  public boolean isAcceptable(Host host) {
    return isRecrawling() || acceptHost(host);
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
      } while (!isAcceptable(host) && this.nextHostIndex.hasNext());

      return Optional.of(host);
    }
    return Optional.empty();
  }

  // TODO: can be a common function with different sql commands
  /** Save entry. Should be called after all processing has been done. */
  boolean saveProcessed(final URI uri, final long length) throws SQLException {
    int rows = 0;
    boolean local = false;
    final Host host = new Host(uri);

    try (var con = this.dataSource.getConnection()) {

      try (var ps =
          con.prepareStatement("INSERT INTO domain_stats (host, url, length) VALUES (?, ?, ?)")) {
        con.setAutoCommit(false);

        ps.setString(1, host.toString());
        ps.setString(2, Manager.removeScheme(uri));
        ps.setLong(3, length);

        rows = ps.executeUpdate();

        if (rows == 0) {
          LOGGER.error("DB insert failed for key {}, zero rows returned.", new Host(uri));
          con.rollback();
          return false;
        }

        local = localInsert(host);
        if (!local) {
          con.rollback();
          return false;
        }

        con.commit();
        return true;

      } catch (SQLException e) {

        LOGGER.error(
            "Failed to insert as DB and local could not be updated, DB: {}, Local: {}",
            rows > 0,
            local);

        if (local) {
          rollbackLocalInsert(host);
        }

        con.rollback();

        if (PG_UNIQUE_VIOLATION_ERR_CODE.equals(e.getSQLState())) {
          LOGGER.info("Rejecting duplicate: {}", uri);
        } else {
          throw e;
        }
      } finally {
        con.setAutoCommit(true);
      }
    }
    return false;
  }

  boolean deleteProcessed(final URI uri) throws SQLException {
    int rows = 0;
    boolean local = false;
    final Host host = new Host(uri);

    try (var con = this.dataSource.getConnection()) {

      try (var ps = con.prepareStatement("DELETE FROM domain_stats WHERE host = ? AND url = ?")) {
        con.setAutoCommit(false);

        ps.setString(1, host.toString());
        ps.setString(2, Manager.removeScheme(uri));

        rows = ps.executeUpdate();

        if (rows == 0) {
          LOGGER.error("DB delete failed for key {}, zero rows returned.", new Host(uri));
          con.rollback();
          return false;
        }

        local = localDelete(host);
        if (!local) {
          con.rollback();
          return false;
        }

        con.commit();
        return true;

      } catch (SQLException e) {

        LOGGER.error(
            "Failed to delete as DB and local could not be updated, DB: {}, Local: {}",
            rows > 0,
            local);

        if (local) {
          rollbackLocalDelete(host);
        }

        con.rollback();
        throw e;
      } finally {
        con.setAutoCommit(true);
      }
    }
  }

  private void rollbackLocalInsert(final Host host) {
    this.count.get(host).decrementAndGet();
  }

  private void rollbackLocalDelete(final Host host) {
    this.count.get(host).incrementAndGet();
  }

  private boolean localInsert(final Host host) {
    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() >= limit) {
        return false;
      } else {
        return i.incrementAndGet() <= limit;
      }

    } else {
      LOGGER.error("count must contain host {} but was missing for local insert", host);
      return false;
    }
  }

  private boolean localDelete(final Host host) {
    if (count.containsKey(host)) {
      count.get(host).decrementAndGet();
      return true;
    } else {
      LOGGER.error("count must contain host {} but was missing for local delete", host);
      return false;
    }
  }

  int count(final Host host) {
    if (count.containsKey(host)) {
      return count.get(host).get();
    } else {
      return 0;
    }
  }
}
