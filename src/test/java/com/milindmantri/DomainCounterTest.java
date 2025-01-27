package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.crawler.CrawlerEvent;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DomainCounterTest {

  // NOTE: Ensure PG is running on local and "test" DB exists.

  private static final HikariDataSource datasource =
      new HikariDataSource(new HikariConfig(dbProps()));

  private static final String CREATE_TABLE =
      """
    CREATE TABLE IF NOT EXISTS
      host_count
    (
        host VARCHAR
      , url VARCHAR
    )
    """;

  private static final String DROP_TABLE =
      """
    DROP TABLE IF EXISTS host_count;
    """;

  private final Crawler mockCrawler = Mockito.mock(Crawler.class);

  @AfterAll
  static void closeDataSource() {
    datasource.close();
  }

  @BeforeEach
  void createTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement(CREATE_TABLE)) {
      ps.executeUpdate();
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
    assertThrows(IllegalArgumentException.class, () -> new DomainCounter(-1, datasource));
    assertThrows(IllegalArgumentException.class, () -> new DomainCounter(-1, null));
  }

  @Test
  void stopAfterLimitSingleHost() throws SQLException {
    // should stop after limit is reached
    DomainCounter dc = new DomainCounter(3, datasource);

    assertTrue(
        IntStream.range(0, 3)
            .mapToObj(i -> dc.acceptReference("http://host.com/%d".formatted(i)))
            .allMatch(b -> b));

    assertFalse(dc.acceptReference("http://host.com/4"));
  }

  @Test
  void restoreCountWhenStarting() throws SQLException {

    try (var con = datasource.getConnection();
        var ps =
            con.prepareStatement(
                """
            INSERT INTO
              host_count
            VALUES
                ('host.com', 'http://host.com/1')
              , ('host.com', 'http://host.com/2')
            """)) {
      ps.executeUpdate();
    }

    DomainCounter dc = new DomainCounter(3, datasource);

    assertTrue(dc.acceptReference("http://host.com/3"));

    assertFalse(dc.acceptReference("http://host.com/4"));
  }

  @Test
  void noTableWhenRestoring() throws SQLException {
    dropTable();
    assertDoesNotThrow(() -> new DomainCounter(3, datasource));
  }

  @Test
  void createTableOnCrawlerStart() throws SQLException {
    var dc = new DomainCounter(1, datasource);

    // ensure no table exists
    dropTable();

    dc.accept(new CrawlerEvent.Builder(CrawlerEvent.CRAWLER_INIT_BEGIN, mockCrawler).build());

    String count =
        """
    SELECT COUNT(*) FROM host_count;
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
    var dc = new DomainCounter(1, datasource);

    final String link = "https://www.php.net/new-link";

    assertTrue(dc.acceptReference(link));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM host_count")) {

      var rs = ps.executeQuery();

      assertTrue(rs.next());
      String host = rs.getString(1);
      String url = rs.getString(2);

      assertEquals("www.php.net", host);
      assertEquals(link, url);
    }
  }

  @Test
  void insertEntryOnNewLink2() throws SQLException {
    var dc = new DomainCounter(2, datasource);

    final String link1 = "https://www.php.net/new-link";
    final String link2 = "https://www.php.net/new-link2";

    assertTrue(dc.acceptReference(link1));
    assertTrue(dc.acceptReference(link2));

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM host_count")) {

      var rs = ps.executeQuery();

      assertTrue(rs.next());
      assertEquals("www.php.net", rs.getString(1));
      assertEquals(link1, rs.getString(2));

      assertTrue(rs.next());
      assertEquals("www.php.net", rs.getString(1));
      assertEquals(link2, rs.getString(2));

      assertFalse(rs.next());
    }
  }

  @Test
  void dropTableWhenCrawlingEnds() throws SQLException {
    var dc = new DomainCounter(2, datasource);
    dc.accept(new CrawlerEvent.Builder(CrawlerEvent.CRAWLER_RUN_END, mockCrawler).build());

    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT host, url FROM host_count")) {

      SQLException ex = assertThrows(SQLException.class, ps::executeQuery);
      assertEquals("42P01", ex.getSQLState());
    }
  }

  private static Properties dbProps() {
    var props = new Properties();
    props.put("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
    props.put("username", "postgres");
    props.put("password", "pass");

    return props;
  }
}
