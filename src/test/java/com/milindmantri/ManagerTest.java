package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.doc.CrawlDocInfo;
import com.norconex.collector.core.doc.CrawlDocMetadata;
import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

@TestMethodOrder(MethodOrderer.Random.class)
class ManagerTest {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  private static final HikariDataSource datasource = TestCommons.createTestDataSource();

  private static final String DROP_TABLE =
      """
    DROP TABLE IF EXISTS domain_stats, %s;
    """
          .formatted(JdbcStoreTest.QUEUE_TABLE);

  private static final Crawler mockCrawler = Mockito.mock(Crawler.class);

  @AfterAll
  static void closeDataSource() {
    datasource.close();
  }

  @BeforeEach
  void createTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(Manager.CREATE_DOMAIN_STATS_TABLE);
        var indexPs = con.prepareStatement(Manager.CREATE_DOMAIN_STATS_INDEX)) {
      ps.executeUpdate();
      indexPs.executeUpdate();
    }
  }

  @AfterEach
  void dropTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(DROP_TABLE)) {
      ps.executeUpdate();
    }
  }

  @Test
  void test() {
    System.out.println(CrawlerRunner.URL_NORMALIZER.normalizeURL("http://google.com/hello#world"));
  }

  @Test
  void invalidLimit() {
    assertThrows(IllegalArgumentException.class, () -> new Manager(-1, datasource));
    assertThrows(IllegalArgumentException.class, () -> new Manager(-1, null));

    System.out.println(ThreadLocalRandom.current().ints(21, 0, 21).boxed().toList());
  }

  @Test
  void stopAfterLimitSingleHost() throws SQLException {
    // should stop after limit is reached
    Manager dc = new Manager(3, datasource);

    assertTrue(
        IntStream.range(0, 3)
            .mapToObj("http://host.com/%d"::formatted)
            .peek(s -> dc.initCount(qEvent(s)))
            .allMatch(
                s -> {
                  try {
                    return dc.saveProcessed(URI.create(s), 0);
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));

    assertFalse(dc.saveProcessed(URI.create("http://host.com/4"), 0));
  }

  // TODO: add tests for error handling of insertsIntoDomainStats

  @Test
  void restoreCountWhenStarting() throws SQLException {

    try (var con = datasource.getConnection();
        var ps =
            con.prepareStatement(
                """
            INSERT INTO
              domain_stats
            VALUES
                ('host.com', 'http://host.com/1', 0)
              , ('host.com', 'http://host.com/2', 0)
            """)) {
      ps.executeUpdate();
    }

    Manager dc = new Manager(3, datasource);

    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {
      var crawler = Mockito.mock(ProCrawler.class);
      Mockito.when(crawler.getDataStoreEngine()).thenReturn(es.engine());
      dc.setCrawler(crawler);

      dc.restoreCount();
      assertTrue(dc.saveProcessed(URI.create("http://host.com/3"), 0));
      assertFalse(dc.saveProcessed(URI.create("http://host.com/4"), 0));
    }
  }

  @Test
  void noTableWhenRestoring() throws SQLException {
    dropTable();
    assertDoesNotThrow(() -> new Manager(3, datasource));
  }

  @Test
  void createTableOnCrawlerStart() throws SQLException {
    var dc = new Manager(1, datasource);

    // ensure no table exists
    dropTable();

    dc.createStatsTableIfNotExists();

    String count =
        """
    SELECT COUNT(*) FROM domain_stats;
    """;

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(count)) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  void insertEntryOnNewLink() throws SQLException {
    var dc = new Manager(1, datasource);

    final String link = "https://www.php.net/new-link?hello=work";

    dc.initCount(qEvent(link));
    dc.saveProcessed(URI.create(link), 0);

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM domain_stats")) {

      var rs = ps.executeQuery();

      assertTrue(rs.next());
      String host = rs.getString(1);
      String url = rs.getString(2);

      assertEquals("www.php.net", host);
      assertEquals("www.php.net/new-link?hello=work", url);
    }
  }

  @Test
  void insertEntryOnNewLinkWithFragment() throws SQLException {
    var dc = new Manager(1, datasource);

    final String link = "https://www.php.net/new-link#fragment-data";

    dc.initCount(qEvent(link));
    dc.saveProcessed(URI.create(link), 0);

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM domain_stats")) {

      var rs = ps.executeQuery();

      assertTrue(rs.next());
      String host = rs.getString(1);
      String url = rs.getString(2);

      assertEquals("www.php.net", host);
      assertEquals("www.php.net/new-link", url);
    }
  }

  @Test
  void invalidProps() throws SQLException {
    var dc = new Manager(1, datasource);

    final String link = "https://www.php.net/new-link";

    assertFalse(
        dc.acceptMetadata(
            link,
            new com.norconex.commons.lang.map.Properties(
                Map.of("content-type", List.of("image/jpeg")))));
  }

  @Test
  void invalidProps2() throws SQLException {
    var dc = new Manager(1, datasource);

    final String link = "https://www.php.net/new-link";

    assertFalse(
        dc.acceptMetadata(
            link,
            new com.norconex.commons.lang.map.Properties(
                Map.of("Content-Type", List.of("image/png")))));
  }

  @Test
  void insertEntryOnNewLink2() throws SQLException {
    var dc = new Manager(2, datasource);

    final String link1 = "https://www.php.net/new-link";
    final String link2 = "https://www.php.net/new-link2";

    dc.initCount(qEvent(link1));

    assertTrue(dc.saveProcessed(URI.create(link1), 0));
    assertTrue(dc.saveProcessed(URI.create(link2), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM domain_stats")) {

      var rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("www.php.net", rs.getString(1));
      assertEquals("www.php.net/new-link", rs.getString(2));

      assertTrue(rs.next());
      assertEquals("www.php.net", rs.getString(1));
      assertEquals("www.php.net/new-link2", rs.getString(2));

      assertFalse(rs.next());
    }
  }

  @Test
  void insertEntryOnNewLink2WithFragment() throws SQLException {
    var dc = new Manager(2, datasource);

    final String link1 = "https://www.php.net/new-link";
    final String link2 = "https://www.php.net/new-link#frag-on-same-link";

    dc.initCount(qEvent(link1));
    assertTrue(dc.saveProcessed(URI.create(link1), 0));
    assertFalse(dc.saveProcessed(URI.create(link2), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM domain_stats")) {

      var rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("www.php.net", rs.getString(1));
      assertEquals("www.php.net/new-link", rs.getString(2));
      assertFalse(rs.next());
    }
  }

  @Test
  void dropTableWorks() throws SQLException {
    var dc = new Manager(2, datasource);
    dc.dropStatsTable();

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM domain_stats")) {

      SQLException ex = assertThrows(SQLException.class, ps::executeQuery);
      assertEquals("42P01", ex.getSQLState());
    }
  }

  @Test
  void stopAfterLimitRefFilter() throws SQLException {
    // should stop after limit is reached
    Manager dc = new Manager(3, datasource);

    assertTrue(
        IntStream.range(0, 3)
            .mapToObj("http://host.com/%d"::formatted)
            .peek(s -> dc.initCount(qEvent(s)))
            .allMatch(
                str -> {
                  try {
                    return dc.saveProcessed(URI.create(str), 0);
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));

    assertFalse(dc.saveProcessed(URI.create("http://host.com/4"), 0));
  }

  @Test
  void getNextHost() throws SQLException {
    Manager dc =
        new Manager(
            3, datasource, Stream.of("http://site1.com", "https://site2.com").map(URI::create));

    assertFalse(dc.getNextHost().isEmpty());
  }

  @Test
  void getNextHostVisitedOnce() throws SQLException {
    Manager dc =
        new Manager(
            3, datasource, Stream.of("http://site1.com", "https://site2.com").map(URI::create));

    assertTrue(
        IntStream.range(0, 2)
            .<Function<Integer, String>>mapToObj(i -> (s -> "http://site" + s + ".com/" + i))
            .flatMap(s -> Stream.of(s.apply(2), s.apply(1)))
            .peek(s -> dc.initCount(qEvent(s)))
            .allMatch(
                str -> {
                  try {
                    return dc.saveProcessed(URI.create(str), 0);
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));

    assertEquals(new Host("site1.com"), dc.getNextHost().get());
    assertEquals(new Host("site2.com"), dc.getNextHost().get());
    assertEquals(new Host("site1.com"), dc.getNextHost().get());
  }

  @Test
  void getNextHostLimitReached() throws SQLException {
    String s1 = "http://site1.com", s2 = "https://site2.com";
    Manager dc = new Manager(3, datasource, Stream.of(s1, s2).map(URI::create));

    assertTrue(
        IntStream.range(0, 3)
            .<Function<String, String>>mapToObj(i -> (s -> s + "/" + i))
            .flatMap(s -> Stream.of(s.apply(s2), s.apply(s1)))
            .peek(s -> dc.initCount(qEvent(s)))
            .allMatch(
                str -> {
                  try {
                    return dc.saveProcessed(URI.create(str), 0);
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));

    assertTrue(dc.getNextHost().isEmpty());
  }

  record SiteLink(String site, int link) {}

  static void queue(Manager m, JdbcStore<CrawlDocInfo> store, Stream<SiteLink> sl) {

    sl.map(s -> s.site() + s.link())
        .peek(s -> m.initCount(qEvent(s)))
        .forEach(s -> store.save(s, genDoc(s)));
  }

  static void queueWithoutSave(JdbcStore<CrawlDocInfo> store, Stream<SiteLink> sl)
      throws SQLException {

    try (var con = datasource.getConnection();
        var ps =
            con.prepareStatement(
                """
    INSERT INTO %s(id, host, json) VALUES (?, ?, ?)
    """
                    .formatted(store.tableName()))) {
      sl.map(s -> s.site() + s.link())
          .forEach(
              s -> {
                try {
                  ps.setString(1, s);
                  ps.setString(2, new Host(URI.create(s)).toString());
                  ps.setString(3, "{\"reference\": \"%s\" }".formatted(s));
                  ps.executeUpdate();
                  ps.clearParameters();
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  Optional<URI> pollQueue(JdbcStore<CrawlDocInfo> store) {
    return store.deleteFirst().map(CrawlDocInfo::getReference).map(URI::create);
  }

  @Test
  void getNextHostMissedOnce() throws SQLException {
    final String s1 = "http://site1.com/";
    final String s2 = "http://site2.com/";
    Manager dc = new Manager(3, datasource, Stream.of(s1, s2).map(URI::create));

    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {

      var store = es.store();
      queue(
          dc,
          store,
          Stream.of(
              new SiteLink(s1, 1),
              new SiteLink(s2, 1),
              new SiteLink(s2, 2),
              new SiteLink(s2, 3),
              new SiteLink(s2, 4),
              new SiteLink(s2, 5)));

      // next s1
      assertEquals(new Host("site1.com"), new Host(pollQueue(store).get()));
      // next s2
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
      // next s1 -> missed
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
      // next s2
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
      // added s1
      queueWithoutSave(store, Stream.of(new SiteLink(s1, 2)));
      // next s1
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
    }
  }

  @Test
  void getNextHostQueued() throws SQLException {
    final String s1 = "http://site1.com/", s2 = "https://site2.com/";
    Manager dc = new Manager(3, datasource, Stream.of(s1, s2).map(URI::create));

    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {

      var store = es.store();
      queue(
          dc,
          store,
          Stream.of(
              new SiteLink(s1, 1),
              new SiteLink(s2, 1),
              new SiteLink(s2, 2),
              new SiteLink(s2, 3),
              new SiteLink(s2, 4),
              new SiteLink(s2, 5)));

      // next s1
      assertEquals(new Host("site1.com"), new Host(pollQueue(store).get()));
      // next s2
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
      // next s1 -> missed
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
      // next s2
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
      // added s1
      queue(dc, store, Stream.of(new SiteLink(s1, 2)));
      // next s1
      assertEquals(new Host("site1.com"), new Host(pollQueue(store).get()));
      // next s2
      assertEquals(new Host("site2.com"), new Host(pollQueue(store).get()));
    }
  }

  @Test
  void saveProcessedDuplicateUrl() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertTrue(dc.saveProcessed(URI.create(url), 0));
    assertFalse(dc.saveProcessed(URI.create(url), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    assertEquals(1, dc.count(new Host("example.com")));
  }

  @Test
  void saveProcessedInvalidUri() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertThrows(
        IllegalArgumentException.class, () -> dc.saveProcessed(URI.create(":invalid:/uri"), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }

    assertEquals(0, dc.count(new Host("example.com")));
  }

  @Test
  void saveProcessedWithoutQueueing() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    assertFalse(dc.saveProcessed(URI.create(url), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }

    assertEquals(0, dc.count(new Host("example.com")));
  }

  @Test
  void saveProcessedWhenLimitReached() throws SQLException {
    var dc = new Manager(2, datasource);
    var host = "example.com";

    // First two URLs should succeed
    assertTrue(
        IntStream.rangeClosed(1, 2)
            .mapToObj(i -> "http://" + host + "/page" + i)
            .peek(s -> dc.initCount(qEvent(s)))
            .allMatch(
                s -> {
                  try {
                    return dc.saveProcessed(URI.create(s), 0);
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));

    // Third URL should fail
    String url = "http://" + host + "/page3";
    dc.initCount(qEvent(url));
    assertFalse(dc.saveProcessed(URI.create(url), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }

    assertEquals(2, dc.count(new Host(host)));
  }

  @Test
  void saveProcessedWithDatabaseError() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));

    // Drop table to simulate database error
    dropTable();

    assertThrows(SQLException.class, () -> dc.saveProcessed(URI.create(url), 0));
    assertEquals(0, dc.count(new Host("example.com")));
  }

  @Test
  void saveProcessedNullUri() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertThrows(NullPointerException.class, () -> dc.saveProcessed(null, 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }

    assertEquals(0, dc.count(new Host("example.com")));
  }

  @Test
  void saveProcessedMultipleHosts() throws SQLException {
    var dc = new Manager(3, datasource);

    // First host
    String url1 = "http://example1.com/page";
    dc.initCount(qEvent(url1));
    assertTrue(dc.saveProcessed(URI.create(url1), 0));

    // Second host
    String url2 = "http://example2.com/page";
    dc.initCount(qEvent(url2));
    assertTrue(dc.saveProcessed(URI.create(url2), 0));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }

    assertEquals(1, dc.count(new Host("example1.com")));
    assertEquals(1, dc.count(new Host("example2.com")));
  }

  @Test
  void saveProcessedRollbackOnError() throws SQLException {
    var ds = Mockito.spy(datasource);
    try (var con = datasource.getConnection()) {

      Connection spyCon = Mockito.spy(con);
      Mockito.when(ds.getConnection()).thenReturn(spyCon);

      Mockito.doThrow(new SQLException()).when(spyCon).commit();

      var dc = new Manager(3, ds);

      String url = "http://example.com/page";

      dc.initCount(qEvent(url));

      assertThrows(SQLException.class, () -> dc.saveProcessed(URI.create(url), 0));
      assertEquals(0, dc.count(new Host("example.com"))); // Should rollback local count
    }
  }

  @Test
  void deleteProcessedBasicCase() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertTrue(dc.saveProcessed(URI.create(url), 0));
    assertEquals(1, dc.count(new Host("example.com")));

    dc.deleteProcessed(URI.create(url));
    assertEquals(0, dc.count(new Host("example.com")));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  void deleteProcessedNonExistentUrl() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";
    String nonExistentUrl = "http://example.com/nonexistent";

    dc.initCount(qEvent(url));
    assertTrue(dc.saveProcessed(URI.create(url), 0));
    assertEquals(1, dc.count(new Host("example.com")));

    dc.deleteProcessed(URI.create(nonExistentUrl));
    assertEquals(1, dc.count(new Host("example.com")));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  void deleteProcessedMultipleUrls() throws SQLException {
    var dc = new Manager(3, datasource);
    String url1 = "http://example.com/page1";
    String url2 = "http://example.com/page2";

    dc.initCount(qEvent(url1));
    dc.initCount(qEvent(url2));
    assertTrue(dc.saveProcessed(URI.create(url1), 0));
    assertTrue(dc.saveProcessed(URI.create(url2), 0));
    assertEquals(2, dc.count(new Host("example.com")));

    dc.deleteProcessed(URI.create(url1));
    assertEquals(1, dc.count(new Host("example.com")));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  void deleteProcessedDatabaseError() throws SQLException {
    var ds = Mockito.spy(datasource);
    try (var con = datasource.getConnection()) {

      var dc = new Manager(3, ds);
      String url = "http://example.com/page";

      dc.initCount(qEvent(url));
      assertTrue(dc.saveProcessed(URI.create(url), 0));
      assertEquals(1, dc.count(new Host("example.com")));

      Connection spyCon = Mockito.spy(con);
      Mockito.when(ds.getConnection()).thenReturn(spyCon);
      Mockito.doThrow(new SQLException()).when(spyCon).commit();

      assertThrows(SQLException.class, () -> dc.deleteProcessed(URI.create(url)));
      assertEquals(1, dc.count(new Host("example.com"))); // Should maintain count on error

      try (var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
        var rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  void deleteProcessedWithNullUri() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertTrue(dc.saveProcessed(URI.create(url), 0));
    assertEquals(1, dc.count(new Host("example.com")));

    assertThrows(NullPointerException.class, () -> dc.deleteProcessed(null));
    assertEquals(1, dc.count(new Host("example.com"))); // Should maintain count
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  void deleteProcessedWithInvalidUri() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertTrue(dc.saveProcessed(URI.create(url), 0));
    assertEquals(1, dc.count(new Host("example.com")));

    assertThrows(
        IllegalArgumentException.class, () -> dc.deleteProcessed(URI.create(":invalid://uri")));
    assertEquals(1, dc.count(new Host("example.com"))); // Should maintain count
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  void deleteProcessedWithTableDrop() throws SQLException {
    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));
    assertTrue(dc.saveProcessed(URI.create(url), 0));
    assertEquals(1, dc.count(new Host("example.com")));

    dropTable();

    assertThrows(SQLException.class, () -> dc.deleteProcessed(URI.create(url)));
    assertEquals(1, dc.count(new Host("example.com"))); // Should maintain count on error
  }

  @Test
  void deleteProcessedMultipleHosts() throws SQLException {
    var dc = new Manager(3, datasource);
    String url1 = "http://example1.com/page";
    String url2 = "http://example2.com/page";

    dc.initCount(qEvent(url1));
    dc.initCount(qEvent(url2));
    assertTrue(dc.saveProcessed(URI.create(url1), 0));
    assertTrue(dc.saveProcessed(URI.create(url2), 0));

    assertEquals(1, dc.count(new Host("example1.com")));
    assertEquals(1, dc.count(new Host("example2.com")));

    dc.deleteProcessed(URI.create(url1));
    assertEquals(0, dc.count(new Host("example1.com")));
    assertEquals(1, dc.count(new Host("example2.com")));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT COUNT(*) FROM domain_stats")) {
      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  void deleteWithoutSave() throws SQLException {

    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    dc.initCount(qEvent(url));

    assertFalse(dc.deleteProcessed(URI.create(url)));
  }

  @Test
  void deleteWithoutAccept() throws SQLException {

    var dc = new Manager(3, datasource);
    String url = "http://example.com/page";

    assertFalse(dc.deleteProcessed(URI.create(url)));
  }

  @Test
  void acceptRefNotQueued() {
    var dc = new Manager(3, datasource);
    assertFalse(dc.acceptReference("http://site.com"));
  }

  @Test
  void acceptRefQueued() {
    var site = "http://site.com";
    var dc = new Manager(3, datasource);
    dc.initCount(qEvent(site));
    assertTrue(dc.acceptReference(site));
  }

  @Test
  void acceptRefRecrawl() {
    var site = "http://site.com";
    var dc = new Manager(3, datasource);
    var crawler = Mockito.mock(ProCrawler.class);
    Mockito.when(crawler.isRecrawling()).thenReturn(true);

    dc.setCrawler(crawler);
    assertTrue(dc.acceptReference(site));
  }

  @Test
  void importerStage() {
    var site = "http://site.com";
    var dc = new Manager(3, datasource);

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(false);
    Mockito.when(context.getDocument().getReference()).thenReturn(site);

    assertTrue(dc.execute(context));
  }

  @Test
  void importerStageCountNotInit() {
    var site = "http://site.com";
    var dc = new Manager(3, datasource);

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn(site);

    // since queue not init
    assertFalse(dc.execute(context));
  }

  @Test
  void importerStageCountInit() {
    var site = "http://site.com";
    var dc = new Manager(3, datasource);

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn(site);

    dc.initCount(qEvent(site));

    assertTrue(dc.execute(context));
  }

  @Test
  void importerStageRecrawlStartUrl() throws SQLException {
    var site = "http://site.com";
    var dc = new Manager(3, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn(site);

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {
      Mockito.when(crwl.getDataStoreEngine()).thenReturn(es.engine());

      assertTrue(dc.execute(context));
    }
  }

  @Test
  void importerStageRecrawlNotCloseStartUrl() throws SQLException {
    var site = "http://site.com";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn(site + "/1234");

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    assertFalse(dc.execute(context));
  }

  @Test
  void importerStageRecrawlNotStartUrl() throws SQLException {
    var site = "http://site.com";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn("http://site2.com");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    assertFalse(dc.execute(context));
  }

  @Test
  void importerStageRecrawlCloseStartUrl() throws SQLException {
    var site = "http://site.com/";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    // notice missing "/" at the end
    Mockito.when(context.getDocument().getReference()).thenReturn("http://site.com");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {
      Mockito.when(crwl.getDataStoreEngine()).thenReturn(es.engine());
      assertTrue(dc.execute(context));
    }
  }

  @Test
  void importerStageRecrawlCloseStartUrl2() throws SQLException {
    var site = "http://site.com/";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    // notice https
    Mockito.when(context.getDocument().getReference()).thenReturn("https://site.com/");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {
      Mockito.when(crwl.getDataStoreEngine()).thenReturn(es.engine());
      assertTrue(dc.execute(context));
    }
  }

  @Test
  void importerStageRecrawlCloseStartUrl3() throws SQLException {
    var site = "http://site.com/";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    // notice https
    Mockito.when(context.getDocument().getReference()).thenReturn("https://site.com/?hello=world");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    assertFalse(dc.execute(context));
  }

  @Test
  void importerStageRecrawlCloseStartUrlFrag() throws SQLException {
    var site = "http://site.com/";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    // notice https
    Mockito.when(context.getDocument().getReference()).thenReturn("https://site.com/#fragment");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    assertFalse(dc.execute(context));
  }

  @Test
  void importerStageRecrawlLaterStartUrl() throws SQLException {
    var site = "http://site.com/";
    var host = new Host(URI.create(site));
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn("http://site.com/");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    // adding entries to queue (can only happen when start url has already been processed)
    try (var es = JdbcStoreTest.EngineStore.queueStore(dc)) {
      Mockito.when(crwl.getDataStoreEngine()).thenReturn(es.engine());
      es.store().save(site + "link2", new CrawlDocInfo(site + "link2"));
      es.store().save(site + "link3", new CrawlDocInfo(site + "link3"));
      es.store().save(site + "link4", new CrawlDocInfo(site + "link4"));

      dc.initCount(qEvent(site));
      // limit reached
      dc.saveProcessed(URI.create("http://site.com/link1"), 0);

      assertFalse(dc.execute(context));
    }
  }

  @Test
  void importerStageRecrawlNonStartUrl() throws SQLException {
    var site = "http://site.com";
    var dc = new Manager(1, datasource, Stream.of(site).map(URI::create));

    var context = Mockito.mock(ImporterPipelineContext.class, RETURNS_DEEP_STUBS);
    // appears new doc since no entry of past
    Mockito.when(context.getDocument().getMetadata().getBoolean(CrawlDocMetadata.IS_CRAWL_NEW))
        .thenReturn(true);
    Mockito.when(context.getDocument().getReference()).thenReturn("http://site.com/hello-world");

    var crwl = Mockito.mock(ProCrawler.class);
    Mockito.when(crwl.isRecrawling()).thenReturn(true);
    dc.setCrawler(crwl);

    dc.initCount(qEvent(site));
    // limit reached
    dc.saveProcessed(URI.create("http://site.com/link1"), 0);

    assertFalse(dc.execute(context));
  }

  @Test
  void randomizeIndices() {
    assertEquals(2, Manager.randomizedIndices(2, 20).findFirst().get());

    assertEquals(19, Manager.randomizedIndices(2, 20).skip(1).distinct().count());

    assertTrue(
        Manager.randomizedIndices(2, 20)
            .skip(1)
            .allMatch(i -> IntStream.range(0, 20).filter(j -> j != 2).anyMatch(j -> j == i)));

    assertTrue(Manager.randomizedIndices(2, 20).skip(1).noneMatch(i -> i == 2));
  }

  @Test
  void randomizeIndicesZero() {
    assertEquals(0, Manager.randomizedIndices(0, 20).findFirst().get());

    assertEquals(19, Manager.randomizedIndices(0, 20).skip(1).distinct().count());

    assertTrue(
        Manager.randomizedIndices(0, 20)
            .skip(1)
            .allMatch(i -> IntStream.range(0, 20).filter(j -> j != 0).anyMatch(j -> j == i)));

    assertTrue(Manager.randomizedIndices(0, 20).skip(1).noneMatch(i -> i == 0));
  }

  @Test
  void randomizeIndicesLast() {
    assertEquals(19, Manager.randomizedIndices(19, 20).findFirst().get());

    assertEquals(19, Manager.randomizedIndices(19, 20).skip(1).distinct().count());

    assertTrue(
        Manager.randomizedIndices(19, 20)
            .skip(1)
            .allMatch(i -> IntStream.range(0, 20).filter(j -> j != 19).anyMatch(j -> j == i)));

    assertTrue(Manager.randomizedIndices(19, 20).skip(1).noneMatch(i -> i == 19));
  }

  static Host qEvent(String link) {
    return new Host(URI.create(link));
  }

  static CrawlDocInfo genDoc(String link) {
    return new CrawlDocInfo(link);
  }
}
