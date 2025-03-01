package com.milindmantri;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.norconex.collector.core.Collector;
import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.doc.CrawlDocInfo;
import com.norconex.commons.lang.map.Properties;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
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

  @BeforeEach
  void drop() throws SQLException {
    TestCommons.exec(
        ds,
        """
  DROP TABLE %s;
  """
            .formatted(QUEUE_TABLE));
  }

  @Test
  void queuedTableWithHost() {
    Crawler crawler = Mockito.mock(Crawler.class);
    Collector collector = Mockito.mock(Collector.class);

    Mockito.when(collector.getId()).thenReturn(COLLECTOR);
    Mockito.when(crawler.getId()).thenReturn(CRAWLER);
    Mockito.when(crawler.getCollector()).thenReturn(collector);

    try (var engine = new JdbcStoreEngine()) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var _ = new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class)) {

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

    try (var engine = new JdbcStoreEngine()) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store = new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class)) {

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

    try (var engine = new JdbcStoreEngine()) {

      engine.setConfigProperties(new Properties(TestCommons.dbProps()));
      engine.init(crawler);

      // creating store should auto create table
      try (var store = new JdbcStore<>(engine, JdbcStore.QUEUED_STORE, CrawlDocInfo.class)) {

        store.save("https://sub.some.com/hello-world", new CrawlDocInfo());

        var maybeRec = store.deleteFirst();
        assertTrue(maybeRec.isPresent());
      }
    }
  }
}
