package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonPrimitive;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class ZonedDateTimeTypeAdapterTest {

  @Test
  void serialize() {
    var adapter = new ZonedDateTimeTypeAdapter();

    var ts = ZonedDateTime.of(LocalDateTime.of(2025, 1, 31, 12, 13, 14), ZoneId.of("Asia/Kolkata"));

    assertEquals(
        new JsonPrimitive("2025-01-31T12:13:14+05:30[Asia/Kolkata]"),
        adapter.serialize(ts, null, null));
  }

  @Test
  void deserialize() {

    var adapter = new ZonedDateTimeTypeAdapter();
    var ts = ZonedDateTime.of(LocalDateTime.of(2025, 1, 31, 12, 13, 14), ZoneId.of("Asia/Kolkata"));

    assertEquals(
        ts,
        adapter.deserialize(
            new JsonPrimitive("2025-01-31T12:13:14+05:30[Asia/Kolkata]"), null, null));
  }
}
