package com.milindmantri;

import com.norconex.collector.core.filter.IMetadataFilter;
import com.norconex.collector.core.filter.impl.MetadataFilter;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.crawler.URLCrawlScopeStrategy;
import com.norconex.commons.lang.text.TextMatcher;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class Main {

  public static void main(String[] args) {

    // Why even allow for new HttpCollector(), when setting id is required. It will anyway error.
    HttpCollectorConfig config = new HttpCollectorConfig();
    config.setId("test");

    config.setMaxConcurrentCrawlers(2);

    HttpCrawlerConfig crawlerConfig = new HttpCrawlerConfig();

    // TODO: pass list of all URLs to crawl
    crawlerConfig.setStartURLs("https://www.php.net", "https://elm-lang.org");
    crawlerConfig.setId("test-crawler");

    // TODO: What about the case for redirected domains, like openjdk.java.net
    var urlCrawlScope = new URLCrawlScopeStrategy();
    urlCrawlScope.setStayOnDomain(true);
    urlCrawlScope.setIncludeSubdomains(false);
    crawlerConfig.setUrlCrawlScopeStrategy(urlCrawlScope);

    crawlerConfig.setFetchHttpHead(true);
    crawlerConfig.setMetadataFilters(getTextOnlyMetadataFilters().toList());

    try (ProsearchJdbcDataStoreEngine engine = new ProsearchJdbcDataStoreEngine()) {

      engine.setConfigProperties(dbProps());
      crawlerConfig.setDataStoreEngine(engine);

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
