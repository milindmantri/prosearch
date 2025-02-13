package com.milindmantri.pages;

import static com.milindmantri.Html.h1;

import com.milindmantri.Html;
import com.milindmantri.Html.Tag;
import com.milindmantri.Html.VoidTag;
import com.milindmantri.Main;
import com.milindmantri.TantivyClient;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class SearchPage {

  private static final Html docType =
      new Html() {
        public String toString() {
          return "<!DOCTYPE html>";
        }
      };

  private static final String DEFAULT_TITLE = "Programmer's Search";

  private static final String CSS_STYLES =
      """
    body {
      max-width: 900px;
      font-family: monospace;
      font-size: 18px;
    }

    form input {
      font-size: 1.2rem;
      padding: 0.5rem;
    }

    form input[type="text"] {
      width: 50%;
    }

    form input[type="submit"] {
      padding-left: 1rem;
      padding-right: 1rem;
    }

    section div {
      margin-bottom: 2rem;
    }

    section div h3 {
      font-size: 120%;
      margin-bottom: 0.5rem;
    }

    section div p {
      margin-top: 0;
      line-height: 150%;
    }
    """;

  private final String title;
  private final String searchTerm;
  private final TantivyClient.SearchResultWithLatency results;

  public SearchPage(final String searchTerm, final TantivyClient.SearchResultWithLatency results) {
    if (searchTerm == null || searchTerm.isBlank()) {
      throw new IllegalArgumentException("searchTerm must not be null or blank.");
    }

    this.searchTerm = searchTerm;
    this.results = Objects.requireNonNull(results, "results must not be null.");
    this.title = "%s | %s".formatted(searchTerm, DEFAULT_TITLE);
  }

  public SearchPage() {
    this.title = DEFAULT_TITLE;
    this.searchTerm = "";
    this.results = null;
  }

  public Stream<Html> html() {
    Stream.Builder<Html> builder = Stream.builder();

    builder.add(docType).add(new Tag("html", Map.of("lang", "en"), Stream.of(head(), body())));

    return builder.build();
  }

  private Html head() {
    return Html.head(
        Stream.of(
            new VoidTag("meta", Map.of("charset", "utf-8")),
            new VoidTag(
                "meta",
                Map.of("name", "viewport", "content", "width=device-width, initial-scale=1.0")),
            Html.title(this.title),
            new Tag("style", CSS_STYLES)));
  }

  private Html body() {
    var builder = Stream.<Html>builder();

    builder
        .add(h1(DEFAULT_TITLE))
        .add(
            Html.formGET(
                "",
                "Search",
                Stream.of(
                    Html.inputText(Main.QUERY_PARAM, this.searchTerm, "Type your text here..."))));

    if (!this.searchTerm.isBlank()) {

      var maybeResults = this.results.results();
      if (maybeResults.isPresent()) {

        builder.add(Html.section(maybeResults.get().map(SearchPage::divFromSearchResult)));

      } else {
        // no results found
        builder.add(Html.h3("Sorry, no search results found!"));
      }

      builder.add(new VoidTag("hr"));

      builder.add(
          Html.pStrong(
              "Search latency: %.3fms".formatted(this.results.latency().toNanos() / 1_000_000.0)));
    }

    return Html.body(builder.build());
  }

  private static Html divFromSearchResult(final TantivyClient.SearchResult res) {
    return Html.div(
        Stream.of(Html.a(res.url(), true, Stream.of(Html.h3(res.title()))), Html.p(res.snippet())));
  }
}
