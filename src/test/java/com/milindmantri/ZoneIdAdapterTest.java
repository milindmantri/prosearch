package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonObject;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class ZoneIdAdapterTest {

  @Test
  void read() {

    var obj = new JsonObject();
    obj.addProperty("id", "Asia/Kolkata");

    assertEquals(
        ZoneId.of("Asia/Kolkata"), new ZoneIdAdapter().deserialize(obj, ZoneId.class, null));
  }

  @Test
  void readNull() {
    var obj = new JsonObject();
    obj.add("id", null);

    assertNull(new ZoneIdAdapter().deserialize(obj, ZoneId.class, null));
  }
}
