package com.milindmantri;

import static com.milindmantri.Main.write;

import com.milindmantri.pages.StatisticsPage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record StatisticsHttpHandler(DataSource datasource, Set<String> startHosts)
    implements HttpHandler {

  public StatisticsHttpHandler(DataSource ds, Stream<String> startHosts) {
    this(ds, startHosts.collect(Collectors.toSet()));
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsHttpHandler.class);

  static final String GET_STATS_QUERY =
      """
    SELECT
        host
      , count(url) as urls
      , pg_size_pretty( sum(length) ) as size
    FROM
      domain_stats
    GROUP BY
      host
    ORDER BY
      urls DESC
    """;

  public static final String STATISTICS_PAGE_PATH = "/stats/";

  public record Stat(String domain, int links, String prettySize) {}

  @Override
  public void handle(final HttpExchange exchange) throws IOException {

    final URI uri = exchange.getRequestURI();

    exchange.getResponseHeaders().add("Content-Type", "text/html");

    try {
      if (STATISTICS_PAGE_PATH.equals(uri.getPath())) {

        // need to send response headers before calling getResponseBody (API limitation)
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);

        try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
            var writer = new BufferedWriter(outWriter);
            var con = this.datasource.getConnection();
            var ps = con.prepareStatement(GET_STATS_QUERY)) {

          var rs = ps.executeQuery();
          Stream<Stat> summary = getSummary(rs);

          new StatisticsPage(summary).html().map(Html::toHtml).forEach(str -> write(writer, str));
        }

      } else {
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, 0);

        try (var outWriter = new OutputStreamWriter(exchange.getResponseBody());
            var writer = new BufferedWriter(outWriter)) {
          writer.write("Not found.");
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

  // for testing
  Stream<Stat> getSummary(final ResultSet rs) throws SQLException {
    Stream.Builder<Stat> builder = Stream.builder();
    Set<String> zeroCrawls = new HashSet<>(this.startHosts);

    while (rs.next()) {
      String domain = rs.getString("host");
      zeroCrawls.remove(domain);

      int count = rs.getInt("urls");
      String size = rs.getString("size");

      builder.accept(new Stat(domain, count, size));
    }

    zeroCrawls.stream().map(DomainCounter::getHost).map(s -> new Stat(s, 0, "-")).forEach(builder);

    return builder.build();
  }
}
