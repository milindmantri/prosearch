package com.milindmantri;

import com.norconex.collector.core.filter.IReferenceFilter;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DomainCounter implements IReferenceFilter {

  private final int limit;

  private final Map<String, AtomicInteger> count = new ConcurrentHashMap<>();

  public DomainCounter(final int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException(
          "Limit must be greater than zero, but was %d.".formatted(limit));
    }

    this.limit = limit;
  }

  @Override
  public boolean acceptReference(final String reference) {
    String host = URI.create(reference).getHost();

    if (count.containsKey(host)) {
      AtomicInteger i = count.get(host);

      if (i.get() == limit) {
        return false;
      } else {
        return i.incrementAndGet() <= limit;
      }

    } else {
      count.put(host, new AtomicInteger(1));
      return true;
    }
  }
}
