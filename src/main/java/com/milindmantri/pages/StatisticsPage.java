package com.milindmantri.pages;

import com.milindmantri.Html;
import com.milindmantri.StatisticsHttpHandler;
import com.milindmantri.TantivyClient;
import java.util.Map;
import java.util.stream.Stream;

public class StatisticsPage {

  private static final Html docType = () -> "<!DOCTYPE html>";

  private static final String DEFAULT_TITLE = "Statistics | Programmer's Search ";

  private static final String CSS_STYLES =
      """
    body {
      max-width: 900px;
      font-family: monospace;
      font-size: 18px;
    }

    table {
      width: 100%;
      text-align: left;
      border-collapse: collapse;
    }

    td {
      border: 1px solid lightgray;
      padding: 0.7rem 0.5rem;
    }

    th {
      font-size: 120%;
    }
    """;

  private final Stream<StatisticsHttpHandler.Stat> stats;

  public StatisticsPage(final Stream<StatisticsHttpHandler.Stat> stats) {
    if (stats == null) {
      throw new IllegalArgumentException("stats must not be null or blank.");
    }

    this.stats = stats;
  }

  public Stream<Html> html() {
    Stream.Builder<Html> builder = Stream.builder();

    builder.add(docType).add(Html.html(Stream.of(head(), body())));

    return builder.build();
  }

  private Html head() {
    return Html.head(
        Stream.of(
            Html.meta(Map.of("charset", "utf-8")),
            Html.meta(
                Map.of("name", "viewport", "content", "width=device-width, initial-scale=1.0")),
            Html.title(DEFAULT_TITLE),
            Html.style(CSS_STYLES)));
  }

  private Html body() {
    var builder = Stream.<Html>builder();

    var tHead = Html.thead(Stream.of(Html.th("Domain"), Html.th("Indexed Pages"), Html.th("Size")));

    Stream<Html> rows =
        this.stats.map(
            stat ->
                Html.tr(
                    Stream.of(
                        Html.td(stat.domain()),
                        Html.td(String.valueOf(stat.links())),
                        Html.td(stat.prettySize()))));

    var tbody = Html.tbody(rows);

    builder
        .add(
            Html.h1(
                "Statistics | "
                    + Html.a("/search/", false, Stream.of(new Html.Inner("Programmer's Search")))
                        .toHtml()))
        .add(Html.table(tHead, tbody));

    return Html.body(builder.build());
  }

  private static Html divFromSearchResult(final TantivyClient.SearchResult res) {
    return Html.div(
        Stream.of(Html.a(res.url(), true, Stream.of(Html.h3(res.title()))), Html.p(res.snippet())));
  }
}
