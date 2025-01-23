package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class DomainCounterTest {

  @Test
  void invalidLimit() {
    assertThrows(IllegalArgumentException.class, () -> new DomainCounter(-1));
  }

  @Test
  void stopAfterLimitSingleHost() {
    // should stop after limit is reached
    DomainCounter dc = new DomainCounter(3);

    assertTrue(
        IntStream.range(0, 3)
            .mapToObj(i -> dc.acceptReference("http://host.com/%d".formatted(i)))
            .allMatch(b -> b));

    assertFalse(dc.acceptReference("http://host.com/4"));
  }
}
