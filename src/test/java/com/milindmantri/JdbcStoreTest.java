package com.milindmantri;

import static com.milindmantri.ManagerTest.genDoc;
import static com.milindmantri.ManagerTest.qEvent;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.norconex.collector.core.Collector;
import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.doc.CrawlDocInfo;
import com.norconex.collector.core.store.DataStoreException;
import com.norconex.commons.lang.map.Properties;
import com.zaxxer.hikari.HikariDataSource;
import java.io.Closeable;
import java.net.URI;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JdbcStoreTest {

  private static final HikariDataSource ds = TestCommons.createTestDataSource();
  private static final String CRAWLER = "cra";
  private static final String COLLECTOR = "col";
  static final String QUEUE_TABLE =
      "%s_%s_%s".formatted(COLLECTOR, CRAWLER, JdbcStore.QUEUED_STORE);

  static final String CACHE_TABLE =
      "%s_%s_%s".formatted(COLLECTOR, CRAWLER, JdbcStore.CACHED_STORE);

  @AfterAll
  static void close() {
    ds.close();
  }

  @AfterEach
  void drop() throws SQLException {
    TestCommons.exec(
        ds,
        """
  DROP TABLE IF EXISTS %s, %s, domain_stats;
  """
            .formatted(QUEUE_TABLE, CACHE_TABLE));
  }

  @BeforeEach
  void createTable() throws SQLException {
    try (var con = ds.getConnection();
        var ps = con.prepareStatement(Manager.CREATE_DOMAIN_STATS_TABLE);
        var indexPs = con.prepareStatement(Manager.CREATE_DOMAIN_STATS_INDEX)) {
      ps.executeUpdate();
      indexPs.executeUpdate();
    }
  }

  @Test
  void queuedTableWithHost() {

    // creating store should auto create table
    try (var es = EngineStore.queueStore(Mockito.mock(Manager.class))) {
      assertDoesNotThrow(
          () ->
              TestCommons.exec(
                  ds,
                  """
      INSERT INTO %s (id, host, modified, json)
      VALUES ('http://some.com', 'some.com', now(), '{}')
      """
                      .formatted(QUEUE_TABLE)));
    }
  }

  @Test
  void insert() {

    try (var es = EngineStore.queueStore(Mockito.mock(Manager.class))) {

      es.store().save("https://sub.some.com/hello-world", new CrawlDocInfo());

      boolean result =
          TestCommons.<Boolean>query(
              ds,
              """
        SELECT id, host FROM %s
        """
                  .formatted(QUEUE_TABLE),
              rs -> {
                try {
                  assertTrue(rs.next());
                  assertEquals("https://sub.some.com/hello-world", rs.getString(1));
                  assertEquals("sub.some.com", rs.getString(2));

                  return true;
                } catch (SQLException ex) {
                  throw new RuntimeException(ex);
                }
              });

      assertTrue(result);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void deleteFirst() throws SQLException {
    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    try (var engine = new JdbcStoreEngine(Mockito.mock(Manager.class))) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store =
          new JdbcStore<>(
              engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class, Mockito.mock(Manager.class))) {

        store.save("https://sub.some.com/hello-world", new CrawlDocInfo());

        var maybeRec = store.deleteFirst();
        assertTrue(maybeRec.isEmpty());
      }
    }
  }

  @Test
  void deleteFirstNextHost() throws SQLException {
    final String s1 = "http://site1.com";
    final String s2 = "https://site2.com";

    Manager dc = new Manager(3, ds, Stream.of(s1, s2).map(URI::create));
    Stream.of(s1, s2)
        .peek(s -> dc.initCount(qEvent(s)))
        .forEach(
            s -> {
              try {
                dc.saveProcessed(URI.create(s), 0);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });

    try (var es = EngineStore.queueStore(dc)) {
      // creating store should auto create table

      TestCommons.exec(
          ds,
          """
      INSERT INTO %1$s (id, host, modified, json)
      VALUES
      ('%2$s/1', '%3$s', now(), '{"reference": "%2$s/1"}'),
      ('%2$s/2', '%3$s', now() + interval '1 second', '{"reference": "%2$s/2"}'),
      ('%4$s/3', '%5$s', now(), '{"reference": "%4$s/3"}'),
      ('%4$s/4', '%5$s', now() + interval '1 second', '{"reference": "%4$s/4"}')
      """
              .formatted(QUEUE_TABLE, s1, new Host(URI.create(s1)), s2, new Host(URI.create(s2))));

      assertEquals(s1 + "/1", es.store().deleteFirst().get().getReference());
      assertEquals(s2 + "/3", es.store().deleteFirst().get().getReference());
    }
  }

  @Test
  void deleteFirstNextHostNotQueued() throws SQLException {

    final String s1 = "http://site1.com";
    final String s2 = "https://site2.com";

    Manager dc = new Manager(3, ds, Stream.of(s1, s2).map(URI::create));
    Stream.of(s1, s2)
        .peek(s -> dc.initCount(qEvent(s)))
        .forEach(
            s -> {
              try {
                dc.saveProcessed(URI.create(s), 0);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });

    try (var es = EngineStore.queueStore(dc)) {
      TestCommons.exec(
          ds,
          """
      INSERT INTO %1$s (id, host, modified, json)
      VALUES
      ('%2$s/1', '%3$s', now(), '{"reference": "%2$s/1"}'),
      ('%2$s/2', '%3$s', now() + interval '1 second', '{"reference": "%2$s/2"}')
      """
              .formatted(QUEUE_TABLE, s1, new Host(URI.create(s1))));

      assertEquals(s1 + "/1", es.store().deleteFirst().get().getReference());

      assertEquals(new Host(URI.create(s2)), dc.getNextHost().get());

      assertEquals(s1 + "/2", es.store().deleteFirst().get().getReference());
      assertEquals(new Host(URI.create(s2)), dc.getNextHost().get());

      assertTrue(es.store().deleteFirst().isEmpty());
    }
  }

  @Test
  void deleteFirstNextHostLimitReached() throws SQLException {
    final String s1 = "http://site1.com";

    Manager dc = new Manager(2, ds, Stream.of(s1).map(URI::create));
    dc.initCount(qEvent(s1));

    try (var es = EngineStore.queueStore(dc)) {
      var store = es.store();

      TestCommons.exec(
          ds,
          """
      INSERT INTO %1$s (id, host, modified, json)
      VALUES
      ('%2$s',   '%3$s', now(),                       '{"reference": "%2$s"}'),
      ('%2$s/1', '%3$s', now() + interval '1 second', '{"reference": "%2$s/1"}'),
      ('%2$s/2', '%3$s', now() + interval '2 second', '{"reference": "%2$s/3"}'),
      ('%2$s/3', '%3$s', now() + interval '3 second', '{"reference": "%2$s/4"}')
      """
              .formatted(QUEUE_TABLE, s1, new Host(URI.create(s1))));

      assertEquals(s1, store.deleteFirst().get().getReference());
      dc.saveProcessed(URI.create(s1), 0);
      assertEquals(s1 + "/1", store.deleteFirst().get().getReference());
      dc.saveProcessed(URI.create(s1 + "/1"), 0);

      assertTrue(store.deleteFirst().isEmpty());

      assertEquals(
          Integer.valueOf(0),
          TestCommons.query(
              ds,
              "SELECT count(*) from %s".formatted(QUEUE_TABLE),
              rs -> {
                try {
                  return rs.next() ? rs.getInt(1) : 0;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }));
    }
  }

  @Test
  void cachedEmpty() throws SQLException {
    Manager dc = new Manager(2, ds, Stream.of("http://example.com").map(URI::create));
    try (var es = EngineStore.cacheStore(dc)) {
      assertTrue(es.engine().isCacheEmpty());
    }
  }

  @Test
  void cachedNotEmpty() throws SQLException {
    Manager dc = new Manager(2, ds, Stream.of("http://example.com").map(URI::create));
    try (var es = EngineStore.cacheStore(dc)) {
      es.store().save("http://example.com/1", genDoc("http://example.com/1"));
      assertFalse(es.engine().isCacheEmpty());
    }
  }

  @Test
  void cachedDrop() throws SQLException {
    Manager dc = new Manager(2, ds, Stream.of("http://example.com").map(URI::create));
    try (var es = EngineStore.cacheStore(dc)) {
      TestCommons.exec(ds, "DROP TABLE %s".formatted(CACHE_TABLE));
      assertThrows(DataStoreException.class, () -> es.engine().isCacheEmpty());
    }
  }

  @Test
  void limitReachedButRecrawling() throws SQLException {
    String site = "http://example.com/";

    Manager dc = new Manager(1, ds, Stream.of(site).map(URI::create));

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create(site + "1"), 0);

    try (var es = EngineStore.queueStore(dc)) {
      ManagerTest.queue(
          dc,
          es.store(),
          Stream.of(
              new ManagerTest.SiteLink(site, 2),
              new ManagerTest.SiteLink(site, 3),
              new ManagerTest.SiteLink(site, 4)));

      assertTrue(es.store().deleteFirst().isPresent());
      assertTrue(es.store().deleteFirst().isPresent());
      assertEquals(1, es.store().count());
    }
  }

  @Test
  void limitReachedButRecrawling2() throws SQLException {
    String s1 = "http://example.com/";
    String s2 = "http://example2.com/";

    Manager dc = new Manager(1, ds, Stream.of(s1, s2).map(URI::create));

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(s1));
    dc.initCount(qEvent(s2));
    // limit reached
    dc.saveProcessed(URI.create(s1 + "1"), 0);
    dc.saveProcessed(URI.create(s2 + "1"), 0);

    // s21 s22 s23
    try (var es = EngineStore.queueStore(dc)) {
      ManagerTest.queue(
          dc,
          es.store(),
          Stream.of(
              new ManagerTest.SiteLink(s2, 2),
              new ManagerTest.SiteLink(s2, 3),
              new ManagerTest.SiteLink(s2, 4)));

      // missed s1
      assertTrue(es.store().deleteFirst().get().getReference().contains(s2));
      assertEquals(2, es.store().count());
    }
  }

  record EngineStore(JdbcStoreEngine engine, JdbcStore<CrawlDocInfo> store) implements Closeable {

    @Override
    public void close() {
      this.engine.close();
      this.store.close();
    }

    static EngineStore cacheStore(final Manager m) {
      return someStore(JdbcStore.CACHED_STORE, m);
    }

    static EngineStore queueStore(final Manager m) {
      return someStore(JdbcStore.QUEUED_STORE, m);
    }

    private static EngineStore someStore(final String name, final Manager m) {

      Crawler crawler = Mockito.mock(Crawler.class);
      Collector collector = Mockito.mock(Collector.class);

      Mockito.when(collector.getId()).thenReturn(JdbcStoreTest.COLLECTOR);
      Mockito.when(crawler.getId()).thenReturn(JdbcStoreTest.CRAWLER);
      Mockito.when(crawler.getCollector()).thenReturn(collector);

      var e = new JdbcStoreEngine(m);
      e.setConfigProperties(new Properties(TestCommons.dbProps()));
      e.init(crawler);

      var store = new JdbcStore<>(e, name, CrawlDocInfo.class, m);

      return new EngineStore(e, store);
    }
  }
}
