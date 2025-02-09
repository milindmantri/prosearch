package com.milindmantri;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

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

  /** When successful, returned long value is the length of indexed document, otherwise empty */
  Optional<Long> indexAndLength(final URI uri, final String title, final String body)
      throws IOException, InterruptedException {

    var obj = new JsonObject();
    obj.addProperty("url", uri.toString());
    obj.addProperty("title", title);
    obj.addProperty("body", body);

    HttpResponse<String> response =
        this.httpClient.send(
            HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(obj.toString()))
                .header("Content-Type", "application/json")
                .uri(URI.create("%s/index/".formatted(this.host.toString())))
                .build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    if (response.statusCode() == HttpURLConnection.HTTP_OK) {
      return Optional.of(Long.parseLong(response.body()));
    } else {
      return Optional.empty();
    }
  }

  /** When successful, returned long value is the length of indexed document, otherwise empty */
  Optional<Long> indexAndLength(final URI uri, final String body)
      throws IOException, InterruptedException {
    return this.indexAndLength(uri, uri.toString(), body);
  }

  @Deprecated
  boolean index(final URI uri, final String title, final String body)
      throws IOException, InterruptedException {

    return indexAndLength(uri, title, body).isPresent();
  }

  @Deprecated
  boolean index(final URI uri, final String body) throws IOException, InterruptedException {
    return this.index(uri, uri.toString(), body);
  }
}
