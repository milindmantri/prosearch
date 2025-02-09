package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.map.Properties;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class TantivyCommitterTest {

  @Test
  void doUpsert() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);
    Mockito.when(client.index(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client)) {

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
          .index(URI.create("http://example.com"), "Example Title", "content");
    }
  }

  @Test
  void doUpsertDeleteFails() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);
    Mockito.when(client.index(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client)) {

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
    Mockito.when(client.index(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client)) {

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
    Mockito.when(client.index(Mockito.any(), Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client)) {
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
      inOrder.verify(client, times(1)).index(URI.create("http://example.com"), "content");
    }
  }

  @Test
  void nullClient() {
    assertThrows(IllegalArgumentException.class, () -> new TantivyCommitter(null));
  }

  @Test
  void nullUpsert() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new TantivyCommitter(Mockito.mock(TantivyClient.class)).doUpsert(null));
  }

  @Test
  void doDelete() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(true);

    try (var tc = new TantivyCommitter(client)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertDoesNotThrow(() -> tc.doDelete(new DeleteRequest("http://example.com", props)));

      Mockito.verify(client, times(1)).delete(URI.create("http://example.com"));
    }
  }

  @Test
  void doDeleteFailed() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);
    Mockito.when(client.delete(Mockito.any())).thenReturn(false);

    try (var tc = new TantivyCommitter(client)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      assertThrows(
          CommitterException.class,
          () -> tc.doDelete(new DeleteRequest("http://example.com", props)));

      Mockito.verify(client, times(1)).delete(URI.create("http://example.com"));
    }
  }
}
