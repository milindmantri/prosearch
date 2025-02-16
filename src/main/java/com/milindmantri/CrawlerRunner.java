package com.milindmantri;

import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.crawler.URLCrawlScopeStrategy;
import com.norconex.collector.http.delay.impl.GenericDelayResolver;
import com.norconex.collector.http.url.IURLNormalizer;
import com.norconex.collector.http.url.impl.GenericURLNormalizer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

public final class CrawlerRunner implements Runnable {

  private static final int PER_HOST_CRAWLING_LIMIT =
      Integer.parseInt(System.getProperty("per-host-crawling-limit", "10000"));

  public static final IURLNormalizer URL_NORMALIZER = new GenericURLNormalizer();

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

    crawlerConfig.setUrlNormalizer(URL_NORMALIZER);

    // TODO: pass list of all URLs to crawl
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    try (InputStream is = classloader.getResourceAsStream("start-urls");
        var isr = new InputStreamReader(is);
        var reader = new BufferedReader(isr)) {

      crawlerConfig.setStartURLs(reader.lines().toList());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

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

    try (ProsearchJdbcDataStoreEngine engine = new ProsearchJdbcDataStoreEngine()) {

      engine.setConfigProperties(Main.dbProps());
      crawlerConfig.setDataStoreEngine(engine);

      var domainCounter = new DomainCounter(PER_HOST_CRAWLING_LIMIT, this.datasource);
      crawlerConfig.setReferenceFilters(domainCounter);
      crawlerConfig.setEventListeners(domainCounter);
      crawlerConfig.setMetadataFilters(domainCounter);

      crawlerConfig.setCommitters(new TantivyCommitter(this.client, this.datasource));

      config.setCrawlerConfigs(crawlerConfig);

      HttpCollector spider = new HttpCollector(config);

      if (Boolean.parseBoolean(System.getProperty("clean-crawler-data", "false"))) {
        spider.clean();
      }

      spider.start();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
