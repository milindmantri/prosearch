package com.milindmantri;

import static com.milindmantri.ManagerTest.qEvent;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.map.Properties;
import com.zaxxer.hikari.HikariDataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class TantivyCommitterTest {

  private static final HikariDataSource datasource = TestCommons.createTestDataSource();
  private static final Manager manager = new Manager(1, datasource);

  @Test
  void doUpsert() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      final String link1 = "http://example.com";
      manager.initCount(qEvent(link1));
      assertDoesNotThrow(
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      link1,
                      props,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

      InOrder inOrder = inOrder(client);
      inOrder.verify(client, times(1)).delete(URI.create("http://example.com"));
      inOrder
          .verify(client, times(1))
          .indexAndLength(URI.create("http://example.com"), "Example Title", "content");
    }
  }

  @Test
  void doUpsertDesc() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {

      var props =
          new Properties(
              Map.of(
                  "title",
                  List.of("Example Title"),
                  "Description",
                  List.of("Example description")));

      assertDoesNotThrow(
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      "http://example.com",
                      props,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

      InOrder inOrder = inOrder(client);
      inOrder.verify(client, times(1)).delete(URI.create("http://example.com"));
      inOrder
          .verify(client, times(1))
          .indexAndLength(URI.create("http://example.com"), "Example Title", "content");
    }
  }

  @Test
  void doUpsertDeleteFails() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertThrows(
          CommitterException.class,
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      "http://example.com",
                      props,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));
    }
  }

  @Test
  void doUpsertIndexFails() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertThrows(
          CommitterException.class,
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      "http://example.com",
                      props,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));
    }
  }

  @Test
  void upsertEmptyTitle() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any())).thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {
      var emptyProps = new Properties();

      assertDoesNotThrow(
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      "http://example.com",
                      emptyProps,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

      InOrder inOrder = inOrder(client);
      inOrder.verify(client, times(1)).delete(URI.create("http://example.com"));
      inOrder.verify(client, times(1)).indexAndLength(URI.create("http://example.com"), "content");
    }
  }

  @Test
  void upsertEmptyTitleEmptyDescription()
      throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any())).thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {
      var emptyProps = new Properties();

      assertDoesNotThrow(
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      "http://example.com",
                      emptyProps,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

      InOrder inOrder = inOrder(client);
      inOrder.verify(client, times(1)).delete(URI.create("http://example.com"));
      inOrder.verify(client, times(1)).indexAndLength(URI.create("http://example.com"), "content");
    }
  }

  @Test
  void upsertEmptyTitleWithDescription()
      throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any())).thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, manager)) {
      var props = new Properties();

      assertDoesNotThrow(
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      "http://example.com",
                      props,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

      InOrder inOrder = inOrder(client);
      inOrder.verify(client, times(1)).delete(URI.create("http://example.com"));
      inOrder.verify(client, times(1)).indexAndLength(URI.create("http://example.com"), "content");
    }
  }

  @Test
  void nullClient() {
    assertThrows(IllegalArgumentException.class, () -> new TantivyCommitter(null, manager));
  }

  @Test
  void nullDatasource() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new TantivyCommitter(Mockito.mock(TantivyClient.class), null));
  }

  @Test
  void nullUpsert() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new TantivyCommitter(Mockito.mock(TantivyClient.class), manager).doUpsert(null));
  }

  @Test
  void doDelete() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client, manager)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertDoesNotThrow(() -> tc.doDelete(new DeleteRequest("http://example.com", props)));

      Mockito.verify(client, times(1)).delete(URI.create("http://example.com"));
    }
  }

  @Test
  void doDeleteFailed() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);

    try (var tc = new TantivyCommitter(client, manager)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertThrows(
          CommitterException.class,
          () -> tc.doDelete(new DeleteRequest("http://example.com", props)));

      Mockito.verify(client, times(1)).delete(URI.create("http://example.com"));
    }
  }

  @Test
  void upsertInsertsIntoDomainStats()
      throws IOException, InterruptedException, CommitterException, SQLException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(6L));

    try (var tc = new TantivyCommitter(client, manager);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      final String link1 = "http://example.com/hello-world";
      manager.initCount(qEvent(link1));
      assertDoesNotThrow(
          () ->
              tc.doUpsert(
                  new UpsertRequest(
                      link1,
                      props,
                      new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

      var rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals("example.com", rs.getString(1));
      assertEquals("example.com/hello-world", rs.getString(2));
      assertEquals(6, rs.getLong(3));

      assertFalse(rs.next());
    }
  }

  @Test
  void upsertInsertsIntoDomainStatsExistingEntry()
      throws IOException, InterruptedException, CommitterException, SQLException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(6L));

    try (var tc = new TantivyCommitter(client, manager);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

      con.createStatement()
          .executeUpdate(
              """
              INSERT INTO domain_stats (host, url, length)
              VALUES ('example.com', 'example.com/hello-world', 7)
              """);

      try (var es = JdbcStoreTest.EngineStore.queueStore(manager)) {
        var crawler = Mockito.mock(ProCrawler.class);
        Mockito.when(crawler.getDataStoreEngine()).thenReturn(es.engine());
        manager.setCrawler(crawler);

        manager.restoreCount();

        var props = new Properties(Map.of("title", List.of("Example Title")));

        assertDoesNotThrow(
            () ->
                tc.doUpsert(
                    new UpsertRequest(
                        "http://example.com/hello-world",
                        props,
                        new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)))));

        var rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("example.com", rs.getString(1));
        assertEquals("example.com/hello-world", rs.getString(2));
        assertEquals(6, rs.getLong(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void deleteDomainStatsExistingEntry()
      throws IOException, InterruptedException, CommitterException, SQLException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client, manager);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

      con.createStatement()
          .executeUpdate(
              """
          INSERT INTO domain_stats (host, url, length)
          VALUES ('example.com', 'example.com/hello-world', 7)
          """);

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertDoesNotThrow(
          () -> tc.doDelete(new DeleteRequest("http://example.com/hello-world", props)));

      var rs = ps.executeQuery();
      assertFalse(rs.next());
    }
  }

  @Test
  void deleteDomainStatsNonExistingEntry()
      throws IOException, InterruptedException, CommitterException, SQLException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client, manager);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertDoesNotThrow(
          () -> tc.doDelete(new DeleteRequest("http://example.com/hello-world", props)));

      var rs = ps.executeQuery();
      assertFalse(rs.next());
    }
  }

  @AfterAll
  static void closeDataSource() {
    datasource.close();
  }

  @BeforeEach
  void createTable() throws SQLException {
    manager.createStatsTableIfNotExists();
  }

  @AfterEach
  void dropTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("DROP TABLE IF EXISTS domain_stats")) {
      ps.executeUpdate();
    }
  }
}
