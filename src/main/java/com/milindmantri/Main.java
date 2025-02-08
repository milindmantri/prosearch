package com.milindmantri;

import com.norconex.collector.core.filter.IMetadataFilter;
import com.norconex.collector.core.filter.impl.MetadataFilter;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.crawler.URLCrawlScopeStrategy;
import com.norconex.commons.lang.text.TextMatcher;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class Main {

  private static final int PER_HOST_CRAWLING_LIMIT = 10_000;

  public static void main(String[] args) throws SQLException {
    // Why even allow for new HttpCollector(), when setting id is required. It will anyway error.
    HttpCollectorConfig config = new HttpCollectorConfig();
    config.setId("test");

    // TODO: crawler automatically closes after finish. We want to recrawl routinely.

    HttpCrawlerConfig crawlerConfig = new HttpCrawlerConfig();

    // TODO: pass list of all URLs to crawl
    crawlerConfig.setStartURLs("https://www.php.net", "https://elm-lang.org");

    // TODO: Set threads for crawler

    crawlerConfig.setId("test-crawler");

    // TODO: What about the case for redirected domains, like openjdk.java.net
    var urlCrawlScope = new URLCrawlScopeStrategy();
    urlCrawlScope.setStayOnDomain(true);
    urlCrawlScope.setIncludeSubdomains(false);
    crawlerConfig.setUrlCrawlScopeStrategy(urlCrawlScope);

    crawlerConfig.setIgnoreCanonicalLinks(true);
    crawlerConfig.setFetchHttpHead(true);
    crawlerConfig.setMetadataFilters(getTextOnlyMetadataFilters().toList());

    try (ProsearchJdbcDataStoreEngine engine = new ProsearchJdbcDataStoreEngine();
        var dataSource = new HikariDataSource(new HikariConfig(dbProps().toProperties()))) {

      engine.setConfigProperties(dbProps());
      crawlerConfig.setDataStoreEngine(engine);

      var domainCounter = new DomainCounter(PER_HOST_CRAWLING_LIMIT, dataSource);
      crawlerConfig.setEventListeners(domainCounter);
      crawlerConfig.setReferenceFilters(domainCounter);

      crawlerConfig.setCommitters(
          new TantivyCommitter(
              new TantivyClient(
                  HttpClient.newBuilder()
                      .followRedirects(HttpClient.Redirect.NORMAL)
                      .version(HttpClient.Version.HTTP_1_1)
                      .build(),
                  URI.create(System.getProperty("tantivy-server", "http://localhost:3000")))));

      config.setCrawlerConfigs(crawlerConfig);

      HttpCollector spider = new HttpCollector(config);

      // TODO: Remove once testing is complete
      spider.clean();

      spider.start();
    }
  }

  private static com.norconex.commons.lang.map.Properties dbProps() {
    var props = new HashMap<String, List<String>>();
    props.put("jdbcUrl", List.of("jdbc:postgresql://localhost:5432/milind"));
    props.put("username", List.of("postgres"));
    props.put("password", List.of("pass"));

    return new com.norconex.commons.lang.map.Properties(props);
  }

  private static Stream<IMetadataFilter> getTextOnlyMetadataFilters() {
    return Stream.of("text/html", "application/xhtml+xml", "text/plain")
        .map(t -> t + "*")
        .mapMulti(
            (t, c) -> {
              c.accept(
                  new MetadataFilter(TextMatcher.basic("Content-Type"), TextMatcher.wildcard(t)));
              c.accept(
                  new MetadataFilter(TextMatcher.basic("content-type"), TextMatcher.wildcard(t)));
            });
  }
}
