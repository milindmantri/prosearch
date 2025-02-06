package com.milindmantri;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public class TantivyClient {

  private final HttpClient httpClient;
  private final URI host;

  public TantivyClient(final HttpClient httpClient, final URI host) {
    // TODO: Add null checks
    this.httpClient = httpClient;
    this.host = host;
  }

  boolean delete(final URI uri) throws IOException, InterruptedException {

    HttpResponse<String> response =
        this.httpClient.send(
            HttpRequest.newBuilder()
                .GET()
                .uri(
                    URI.create("%s/delete/?url=%s".formatted(this.host.toString(), uri.toString())))
                .build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    return response.statusCode() == HttpURLConnection.HTTP_OK && "true".equals(response.body());
  }

  boolean index(final URI uri, final String title, final String body) {
    return true;
  }

  boolean index(final URI uri, final String body) {
    return true;
  }
}
