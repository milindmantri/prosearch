package com.milindmantri;

import com.norconex.collector.core.filter.IMetadataFilter;
import com.norconex.collector.core.filter.impl.MetadataFilter;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.crawler.URLCrawlScopeStrategy;
import com.norconex.collector.http.delay.impl.GenericDelayResolver;
import com.norconex.collector.http.url.impl.GenericURLNormalizer;
import com.norconex.commons.lang.text.TextMatcher;
import java.sql.SQLException;
import java.time.Duration;
import java.util.stream.Stream;
import javax.sql.DataSource;

public final class CrawlerRunner implements Runnable {

  private static final int PER_HOST_CRAWLING_LIMIT = 10_000;

  private final DataSource datasource;
  private final TantivyClient client;

  public CrawlerRunner(final DataSource datasource, final TantivyClient client) {
    if (datasource == null) {
      throw new IllegalArgumentException("datasource must not be null.");
    }

    if (client == null) {
      throw new IllegalArgumentException("client must not be null.");
    }

    this.client = client;
    this.datasource = datasource;
  }

  @Override
  public void run() {
    // Why even allow for new HttpCollector(), when setting id is required. It will anyway error.
    HttpCollectorConfig config = new HttpCollectorConfig();
    config.setId(System.getProperty("collector-id", "ps"));

    HttpCrawlerConfig crawlerConfig = new HttpCrawlerConfig();

    // TODO: Delete orphan URLs and spoiled refs

    crawlerConfig.setUrlNormalizer(new GenericURLNormalizer());

    // TODO: pass list of all URLs to crawl
    crawlerConfig.setStartURLs("https://www.php.net", "https://elm-lang.org");

    crawlerConfig.setNumThreads(Runtime.getRuntime().availableProcessors() * 2);

    GenericDelayResolver delayResolver = new GenericDelayResolver();
    int delayInSeconds = Integer.parseInt(System.getProperty("crawl-download-delay-seconds", "1"));

    delayResolver.setDefaultDelay(Duration.ofSeconds(delayInSeconds).toMillis());
    crawlerConfig.setDelayResolver(delayResolver);

    crawlerConfig.setId(System.getProperty("crawler-id", "crwlr"));

    // TODO: What about the case for redirected domains, like openjdk.java.net
    var urlCrawlScope = new URLCrawlScopeStrategy();
    urlCrawlScope.setStayOnDomain(true);
    urlCrawlScope.setIncludeSubdomains(false);
    crawlerConfig.setUrlCrawlScopeStrategy(urlCrawlScope);

    crawlerConfig.setIgnoreCanonicalLinks(true);
    crawlerConfig.setFetchHttpHead(true);
    crawlerConfig.setMetadataFilters(getTextOnlyMetadataFilters().toList());

    try (ProsearchJdbcDataStoreEngine engine = new ProsearchJdbcDataStoreEngine()) {

      engine.setConfigProperties(Main.dbProps());
      crawlerConfig.setDataStoreEngine(engine);

      var domainCounter = new DomainCounter(PER_HOST_CRAWLING_LIMIT, this.datasource);
      crawlerConfig.setEventListeners(domainCounter);
      crawlerConfig.setReferenceFilters(domainCounter);

      crawlerConfig.setCommitters(new TantivyCommitter(this.client, this.datasource));

      config.setCrawlerConfigs(crawlerConfig);

      HttpCollector spider = new HttpCollector(config);

      // TODO: Remove once testing is complete
      spider.clean();

      spider.start();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
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
