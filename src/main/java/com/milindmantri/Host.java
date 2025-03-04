package com.milindmantri;

import java.net.URI;
import java.net.URISyntaxException;

public record Host(String host) {
  public Host {
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("host must not be null or blank.");
    }

    try {
      new URI(null, host, null, null);

    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid host: " + host, e);
    }
  }

  public Host(URI uri) {
    this(validate(uri.getRawAuthority()));
  }

  private static String validate(String authority) {
    if (authority == null) {
      throw new IllegalArgumentException("uri does not have a valid host.");
    }

    return authority;
  }

  public String toString() {
    return this.host;
  }
}
