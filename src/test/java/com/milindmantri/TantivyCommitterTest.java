package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.map.Properties;
import com.zaxxer.hikari.HikariConfig;
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

  // NOTE: Ensure PG is running on local and "test" DB exists.

  private static final HikariDataSource datasource =
      new HikariDataSource(new HikariConfig(dbProps()));

  @Test
  void doUpsert() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, datasource)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

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
  void doUpsertDesc() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, datasource)) {

      var props =
          new Properties(
              Map.of(
                  "title",
                  List.of("Example Title"),
                  "description",
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
          .indexAndLength(
              URI.create("http://example.com"), "Example Title", "content", "Example description");
    }
  }

  @Test
  void doUpsertDeleteFails() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);
    Mockito.when(client.indexAndLength(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, datasource)) {

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

    try (var tc = new TantivyCommitter(client, datasource)) {

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

    try (var tc = new TantivyCommitter(client, datasource)) {
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

    try (var tc = new TantivyCommitter(client, datasource)) {
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
    Mockito.when(
            client.indexAndLengthNoTitleWithDescription(
                Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(7L));

    try (var tc = new TantivyCommitter(client, datasource)) {
      var props = new Properties();
      props.add("description", "meta description");

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
          .indexAndLengthNoTitleWithDescription(
              URI.create("http://example.com"), "content", "meta description");
    }
  }

  @Test
  void nullClient() {
    assertThrows(IllegalArgumentException.class, () -> new TantivyCommitter(null, datasource));
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
        () -> new TantivyCommitter(Mockito.mock(TantivyClient.class), datasource).doUpsert(null));
  }

  @Test
  void doDelete() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client, datasource)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertDoesNotThrow(() -> tc.doDelete(new DeleteRequest("http://example.com", props)));

      Mockito.verify(client, times(1)).delete(URI.create("http://example.com"));
    }
  }

  @Test
  void doDeleteFailed() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);

    try (var tc = new TantivyCommitter(client, datasource)) {

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

    try (var tc = new TantivyCommitter(client, datasource);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

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
      assertEquals("http://example.com", rs.getString(1));
      assertEquals("http://example.com/hello-world", rs.getString(2));
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

    try (var tc = new TantivyCommitter(client, datasource);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

      con.createStatement()
          .executeUpdate(
              """
              INSERT INTO domain_stats (host, url, length)
              VALUES ('http://example.com', 'http://example.com/hello-world', 7)
              """);

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
      assertEquals("http://example.com", rs.getString(1));
      assertEquals("http://example.com/hello-world", rs.getString(2));
      assertEquals(6, rs.getLong(3));
      assertFalse(rs.next());
    }
  }

  @Test
  void deleteDomainStatsExistingEntry()
      throws IOException, InterruptedException, CommitterException, SQLException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client, datasource);
        var con = datasource.getConnection();
        var ps = con.prepareStatement("SELECT * FROM domain_stats")) {

      con.createStatement()
          .executeUpdate(
              """
          INSERT INTO domain_stats (host, url, length)
          VALUES ('http://example.com', 'http://example.com/hello-world', 7)
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

    try (var tc = new TantivyCommitter(client, datasource);
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
    Main.createStatsTableIfNotExists(datasource);
  }

  @AfterEach
  void dropTable() throws SQLException {
    try (var con = datasource.getConnection();
        var ps = con.prepareStatement("DROP TABLE IF EXISTS domain_stats")) {
      ps.executeUpdate();
    }
  }

  private static java.util.Properties dbProps() {
    var props = new java.util.Properties();
    props.put("jdbcUrl", "jdbc:postgresql://localhost:5432/test");
    props.put("username", "postgres");
    props.put("password", "pass");

    return props;
  }
}
