package com.milindmantri;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class TantivyClient {

  private final HttpClient httpClient;
  private final URI host;

  public TantivyClient(final HttpClient httpClient, final URI host) {
    // TODO: Add null checks
    this.httpClient = httpClient;
    this.host = host;
  }

  public static class FailedSearchException extends Exception {
    public FailedSearchException(final String term) {
      super("Failed to search for term: %s".formatted(term));
    }
  }

  public record SearchResult(String title, String snippet, String url) {}

  public record SearchResultWithLatency(Optional<Stream<SearchResult>> results, Duration latency) {

    // soft equals, does not compare streams
    public boolean equals(Object that) {
      if (this == that) return true;

      return (that instanceof SearchResultWithLatency s)
          && Objects.equals(this.latency, s.latency)
          && this.results.isPresent() == s.results.isPresent();
    }
  }

  // TODO: This could ideally transform JSON response on the fly into a stream of SearchResult
  // which could then be consumed
  SearchResultWithLatency search(final String term)
      throws IOException, InterruptedException, FailedSearchException {

    HttpResponse<String> response =
        this.httpClient.send(
            HttpRequest.newBuilder()
                .GET()
                .uri(URI.create("%s/api/?q=%s".formatted(this.host.toString(), term)))
                .build(),
            HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() == HttpURLConnection.HTTP_OK) {
      JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();

      Duration latency =
          Duration.ofNanos(
              json.getAsJsonObject("timings")
                      .getAsJsonArray("timings")
                      .get(0)
                      .getAsJsonObject()
                      .get("duration")
                      .getAsLong()
                  * 1000L);

      var hits = json.getAsJsonArray("hits");
      if (!hits.isEmpty()) {
        Stream.Builder<SearchResult> results = Stream.builder();

        hits.forEach(
            hit -> {
              var obj = hit.getAsJsonObject();
              var doc = obj.getAsJsonObject("doc");
              results.accept(
                  new SearchResult(
                      doc.getAsJsonArray("title").get(0).getAsString(),
                      obj.getAsJsonPrimitive("snip").getAsString(),
                      doc.getAsJsonArray("url").get(0).getAsString()));
            });

        return new SearchResultWithLatency(Optional.of(results.build()), latency);

      } else {
        return new SearchResultWithLatency(Optional.empty(), latency);
      }

    } else {
      throw new FailedSearchException(term);
    }
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
  Optional<Long> indexAndLength(
      final URI uri, final String title, final String body, final String description)
      throws IOException, InterruptedException {

    var obj = new JsonObject();
    obj.addProperty("url", uri.toString());
    obj.addProperty("title", title);
    obj.addProperty("body", body);
    obj.addProperty("desc", description);

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

  /** When successful, returned long value is the length of indexed document, otherwise empty */
  Optional<Long> indexAndLengthNoTitleWithDescription(
      final URI uri, final String body, final String description)
      throws IOException, InterruptedException {
    return this.indexAndLength(uri, uri.toString(), body, description);
  }
}
