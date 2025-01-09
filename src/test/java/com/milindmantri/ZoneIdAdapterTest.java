package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class ZoneIdAdapterTest {

  @org.junit.jupiter.api.Test
  void read() throws IOException {

    String json =
        """
    { "id" : "Asia/Kolkata" }
    """;

    assertEquals(
        ZoneId.of("Asia/Kolkata"),
        new ZoneIdAdapter().read(new JsonReader(new StringReader(json))));
  }

  @Test
  void readNull() throws IOException {

    String json =
        """
    { "id" : null }
    """;

    assertEquals(null, new ZoneIdAdapter().read(new JsonReader(new StringReader(json))));
  }
}
