package com.milindmantri;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.norconex.collector.core.Collector;
import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.doc.CrawlDocInfo;
import com.norconex.commons.lang.map.Properties;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.SQLException;
import java.util.Optional;
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
  private static final String QUEUE_TABLE =
      "%s_%s_%s".formatted(COLLECTOR, CRAWLER, JdbcStore.QUEUED_STORE);

  @AfterAll
  static void close() {
    ds.close();
  }

  @AfterEach
  void drop() throws SQLException {
    TestCommons.exec(
        ds,
        """
  DROP TABLE %s;
  """
            .formatted(QUEUE_TABLE));
  }

  @BeforeEach
  void createTable() throws SQLException {
    try (var con = ds.getConnection();
      var ps = con.prepareStatement(DomainCounter.CREATE_TABLE);
      var indexPs = con.prepareStatement(DomainCounter.CREATE_INDEX)) {
      ps.executeUpdate();
      indexPs.executeUpdate();
    }
  }

  @Test
  void queuedTableWithHost() {
    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    try (var engine = new JdbcStoreEngine(Mockito.mock(DomainCounter.class))) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.
        init(crawler);

      // creating store should auto create table
      try (var _ =
          new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class, Mockito.mock(DomainCounter.class))) {

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
  }

  @Test
  void insert() {

    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    try (var engine = new JdbcStoreEngine(Mockito.mock(DomainCounter.class))) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store =
          new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class, Mockito.mock(DomainCounter.class))) {

        store.save("https://sub.some.com/hello-world", new CrawlDocInfo());

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
  }

  @Test
  void deleteFirst() throws SQLException {
    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    try (var engine = new JdbcStoreEngine(Mockito.mock(DomainCounter.class))) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store =
          new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class, Mockito.mock(DomainCounter.class))) {

        store.save("https://sub.some.com/hello-world", new CrawlDocInfo());

        var maybeRec = store.deleteFirst();
        assertTrue(maybeRec.isPresent());
      }
    }
  }

  @Test
  void deleteFirstNextHost() throws SQLException {
    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    // TODO: Add Host type and move increment to other function of DomainCounter
    final String s1 = "http://site1.com";
    final String s2 = "https://site2.com";

    DomainCounter dc = new DomainCounter(3, ds, Stream.of(s1, s2).map(URI::create));
    dc.acceptMetadata(s1,TestCommons.VALID_PROPS);
    dc.acceptMetadata(s2, TestCommons.VALID_PROPS);

    try (var engine = new JdbcStoreEngine(dc)) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store =
          new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class, dc)) {

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
            .formatted(
              QUEUE_TABLE,
              s1,
              new Host(URI.create(s1)),
              s2,
          new Host(URI.create(s2))));

        assertEquals(s1 + "/1", store.deleteFirst().get().getReference());
        assertEquals(s2 + "/3", store.deleteFirst().get().getReference());
      }
    }
  }

  @Test
  void deleteFirstNextHostNotQueued() throws SQLException {
    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    final String s1 = "http://site1.com";
    final String s2 = "https://site2.com";

    DomainCounter dc = new DomainCounter(3, ds, Stream.of(s1, s2).map(URI::create));
    dc.acceptMetadata(s1,TestCommons.VALID_PROPS);
    dc.acceptMetadata(s2, TestCommons.VALID_PROPS);

    try (var engine = new JdbcStoreEngine(dc)) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store =
        new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class, dc)) {

        TestCommons.exec(
          ds,
          """
      INSERT INTO %1$s (id, host, modified, json)
      VALUES
      ('%2$s/1', '%3$s', now(), '{"reference": "%2$s/1"}'),
      ('%2$s/2', '%3$s', now() + interval '1 second', '{"reference": "%2$s/2"}')
      """
            .formatted(
              QUEUE_TABLE,
              s1,
              new Host(URI.create(s1))
            ));

        assertEquals(s1 + "/1", store.deleteFirst().get().getReference());
        assertEquals(s1 + "/2", store.deleteFirst().get().getReference());

        assertEquals(new Host(URI.create(s1)), dc.getNextHost().get());
        assertEquals(new Host(URI.create(s1)), dc.getNextHost().get());
      }
    }
  }
}
