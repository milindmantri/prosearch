package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.map.Properties;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TantivyCommitterTest {

  @Test
  void doUpsert() throws CommitterException {
    AtomicInteger deletes = new AtomicInteger();
    AtomicInteger indexed = new AtomicInteger();

    try (var tc =
        new TantivyCommitter(
            new TantivyClient() {
              private static final URI URL = URI.create("http://example.com");

              boolean delete(final URI uri) {
                assertEquals(URL, uri);
                deletes.incrementAndGet();
                return true;
              }

              boolean index(final URI uri, final String title, final String body) {
                assertEquals(URL, uri);
                assertEquals("Example Title", title);
                assertEquals("content", body);
                indexed.incrementAndGet();
                return true;
              }
            })) {

      var props = new Properties(Map.of("title", List.of("Example Title")));

      tc.doUpsert(
          new UpsertRequest(
              "http://example.com",
              props,
              new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8))));

      assertEquals(1, deletes.get());
      assertEquals(1, indexed.get());
    }
  }
}
