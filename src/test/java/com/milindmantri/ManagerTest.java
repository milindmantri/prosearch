package com.milindmantri;

import static com.milindmantri.TestCommons.VALID_PROPS;
import static org.junit.jupiter.api.Assertions.*;

import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.crawler.CrawlerEvent;
import com.norconex.collector.core.doc.CrawlDocInfo;
import com.norconex.commons.lang.event.Event;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ManagerTest {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  private static final HikariDataSource datasource = TestCommons.createTestDataSource();

  private static final String DROP_TABLE =
      """
    DROP TABLE IF EXISTS domain_stats;
    """;

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
  void invalidLimit() {
    assertThrows(IllegalArgumentException.class, () -> new Manager(-1, datasource));
    assertThrows(IllegalArgumentException.class, () -> new Manager(-1, null));
  }

  @Test
  void stopAfterLimitSingleHost() throws SQLException {
    // should stop after limit is reached
    Manager dc = new Manager(3, datasource);

    assertTrue(
        IntStream.range(0, 3)
            .mapToObj("http://host.com/%d"::formatted)
            .peek(s -> dc.accept(qEvent(s)))
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

    var engine = Mockito.mock(JdbcStoreEngine.class);
    Mockito.when(engine.hasQueuedTable()).thenReturn(false);

    dc.restoreCount(engine);
    assertTrue(dc.saveProcessed(URI.create("http://host.com/3"), 0));
    assertFalse(dc.saveProcessed(URI.create("http://host.com/4"), 0));
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

    dc.accept(qEvent(link));
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

    dc.accept(qEvent(link));
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

    dc.accept(qEvent(link1));

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

    dc.accept(qEvent(link1));
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
            .peek(s -> dc.accept(qEvent(s)))
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

    assertTrue(dc.getNextHost().isEmpty());
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
            .peek(s -> dc.accept(qEvent(s)))
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
    Manager dc =
        new Manager(
            3, datasource, Stream.of("http://site1.com", "https://site2.com").map(URI::create));

    assertTrue(
        IntStream.range(0, 3)
            .<Function<Integer, String>>mapToObj(i -> (s -> "http://site" + s + ".com/" + i))
            .flatMap(s -> Stream.of(s.apply(2), s.apply(1)))
            .allMatch(str -> dc.acceptReference(str) && dc.acceptMetadata(str, VALID_PROPS)));

    assertTrue(dc.getNextHost().isEmpty());
  }

  @Test
  void getNextHostMissedOnce() throws SQLException {
    Manager dc =
        new Manager(
            3, datasource, Stream.of("http://site1.com", "https://site2.com").map(URI::create));

    assertTrue(
        IntStream.range(0, 2)
            .<Function<Integer, String>>mapToObj(i -> (s -> "http://site" + s + ".com/" + i))
            .flatMap(s -> Stream.of(s.apply(2), s.apply(1)))
            .peek(s -> dc.accept(qEvent(s)))
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

    dc.notQueued(new Host("site1.com"));

    assertEquals(new Host("site2.com"), dc.getNextHost().get());
    assertEquals(new Host("site2.com"), dc.getNextHost().get());
  }

  @Test
  void getNextHostQueued() throws SQLException {
    Manager dc =
        new Manager(
            3, datasource, Stream.of("http://site1.com", "https://site2.com").map(URI::create));

    assertTrue(
        IntStream.range(0, 2)
            .<Function<Integer, String>>mapToObj(i -> (s -> "http://site" + s + ".com/" + i))
            .flatMap(s -> Stream.of(s.apply(2), s.apply(1)))
            .peek(s -> dc.accept(qEvent(s)))
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

    dc.notQueued(new Host("site1.com"));

    assertEquals(new Host("site2.com"), dc.getNextHost().get());
    assertEquals(new Host("site2.com"), dc.getNextHost().get());

    dc.accept(
        new CrawlerEvent.Builder(CrawlerEvent.DOCUMENT_QUEUED, Mockito.mock(Crawler.class))
            .crawlDocInfo(new CrawlDocInfo("http://site1.com/123"))
            .build());

    assertEquals(new Host("site1.com"), dc.getNextHost().get());
    assertEquals(new Host("site2.com"), dc.getNextHost().get());
    assertEquals(new Host("site1.com"), dc.getNextHost().get());
  }

  static Event qEvent(String link) {
    return new CrawlerEvent.Builder(CrawlerEvent.DOCUMENT_QUEUED, mockCrawler)
        .crawlDocInfo(new CrawlDocInfo(link))
        .build();
  }
}
