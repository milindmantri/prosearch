package com.milindmantri;

public record HostCount(Host host, int count) {

  public HostCount {
    if (host == null) {
      throw new IllegalArgumentException("host must not be null");
    }

    if (count < 0) {
      throw new IllegalArgumentException("count must not be less than zero.");
    }
  }
}
