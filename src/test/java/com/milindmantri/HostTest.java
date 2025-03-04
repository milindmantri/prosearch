package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.junit.jupiter.api.Test;

class HostTest {

  @Test
  void test() {
    assertAll(
        () -> assertEquals("host.com", new Host("host.com").toString()),
        () -> assertThrows(IllegalArgumentException.class, () -> new Host("http://host.com")),
        () -> assertEquals("sub.host.com", new Host("sub.host.com").toString()),
        () -> assertEquals("sub-host.com", new Host("sub-host.com").toString()));
  }

  @Test
  void fromUri() {
    assertAll(
        () -> assertThrows(IllegalArgumentException.class, () -> new Host(URI.create("host.com"))),
        () -> assertEquals("host.com", new Host(URI.create("http://host.com")).toString()));
  }
}
