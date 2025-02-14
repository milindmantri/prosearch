package com.milindmantri;

import com.milindmantri.pages.SearchPage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record SearchHttpHandler(TantivyClient tantivyClient) implements HttpHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SearchHttpHandler.class);

  public static final String QUERY_PARAM = "q";

  @Override
  public void handle(final HttpExchange exchange) throws IOException {
    final URI uri = exchange.getRequestURI();
    final List<NameValuePair> nvp =
        URLEncodedUtils.parse(uri.getRawQuery(), StandardCharsets.UTF_8);

    final var maybeSearchTerm =
        nvp.stream()
            .filter(n -> QUERY_PARAM.equals(n.getName()))
            .map(NameValuePair::getValue)
            .filter(str -> !str.isBlank())
            .findFirst();

    exchange.getResponseHeaders().add("Content-Type", "text/html");

    try {
      if (maybeSearchTerm.isPresent()) {
        final String term = maybeSearchTerm.get();

        var searchResult = tantivyClient.search(URLEncoder.encode(term, StandardCharsets.UTF_8));

        var sp = new SearchPage(term, searchResult);

        // need to send response headers before calling getResponseBody (API limitation)
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);

        try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
            var writer = new BufferedWriter(outWriter)) {

          sp.html().map(Html::toHtml).forEach(str -> write(writer, str));
        }

      } else {
        // empty search page
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
        try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
            var writer = new BufferedWriter(outWriter)) {

          new SearchPage().html().map(Html::toHtml).forEach(str -> write(writer, str));
        }
      }

    } catch (Exception e) {
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
      try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
          var writer = new BufferedWriter(outWriter)) {
        LOGGER.error("Failed to process request.", e);
        writer.write("Internal server error. Contact: milind -at- milindmantri -dot- com");
      }
    } finally {
      exchange.close();
    }
  }

  private static void write(final Writer writer, final String str) {
    try {
      writer.write(str);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
