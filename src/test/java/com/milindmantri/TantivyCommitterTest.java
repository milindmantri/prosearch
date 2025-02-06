package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

import com.norconex.committer.core3.CommitterException;
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

    try (var tc = new TantivyCommitter(client)) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      tc.doUpsert(
          new UpsertRequest(
              "http://example.com",
              props,
              new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8))));

      InOrder inOrder = inOrder(client);
      inOrder.verify(client, times(1)).delete(URI.create("http://example.com"));
      inOrder
          .verify(client, times(1))
          .index(URI.create("http://example.com"), "Example Title", "content");
    }
  }

  @Test
  void upsertEmptyTitle() throws CommitterException, IOException, InterruptedException {
    var client = Mockito.mock(TantivyClient.class);

    try (var tc = new TantivyCommitter(client)) {
      var emptyProps = new Properties();

      tc.doUpsert(
          new UpsertRequest(
              "http://example.com",
              emptyProps,
              new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8))));

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
}
