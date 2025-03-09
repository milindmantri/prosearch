package com.milindmantri;

import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.crawler.URLCrawlScopeStrategy;
import com.norconex.collector.http.link.impl.HtmlLinkExtractor;
import com.norconex.collector.http.url.IURLNormalizer;
import com.norconex.collector.http.url.impl.GenericURLNormalizer;
import com.norconex.commons.lang.unit.DataUnit;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;
import javax.sql.DataSource;

public final class CrawlerRunner implements Runnable {

  private static final int PER_HOST_CRAWLING_LIMIT =
      Integer.parseInt(System.getProperty("per-host-crawling-limit", "10000"));

  public static final IURLNormalizer URL_NORMALIZER = new GenericURLNormalizer();

  private final DataSource datasource;
  private final TantivyClient client;
  private final List<URI> startUrls;

  public CrawlerRunner(
      final DataSource datasource, final TantivyClient client, final Stream<URI> startUrls) {
    if (datasource == null) {
      throw new IllegalArgumentException("datasource must not be null.");
    }

    if (client == null) {
      throw new IllegalArgumentException("client must not be null.");
    }

    if (startUrls == null) {
      throw new IllegalArgumentException("startUrls must not be null.");
    }

    this.client = client;
    this.datasource = datasource;
    this.startUrls = startUrls.toList();
  }

  @Override
  public void run() {
    // Why even allow for new HttpCollector(), when setting id is required. It will anyway error.
    HttpCollectorConfig config = new HttpCollectorConfig();
    config.setId(System.getProperty("collector-id", "ps"));

    HttpCrawlerConfig crawlerConfig = new HttpCrawlerConfig();
    int mmi =
        DataUnit.MB
            .toBytes(Integer.parseInt(System.getProperty("crwlr-max-memory-inst", "50")))
            .intValue();
    int mmp =
        DataUnit.MB
            .toBytes(Integer.parseInt(System.getProperty("crwlr-max-memory-pool", "500")))
            .intValue();

    config.setMaxMemoryInstance(mmi);

    config.setMaxMemoryPool(mmp);

    // TODO: Delete orphan URLs and spoiled refs

    crawlerConfig.setUrlNormalizer(URL_NORMALIZER);

    crawlerConfig.setStartURLs(this.startUrls.stream().map(URI::toString).toList());

    //    crawlerConfig.setNumThreads(Runtime.getRuntime().availableProcessors() * 2);
    crawlerConfig.setNumThreads(1);

    crawlerConfig.setId(System.getProperty("crawler-id", "crwlr"));

    // TODO: What about the case for redirected domains, like openjdk.java.net
    var urlCrawlScope = new URLCrawlScopeStrategy();
    urlCrawlScope.setStayOnDomain(true);
    urlCrawlScope.setIncludeSubdomains(false);
    crawlerConfig.setUrlCrawlScopeStrategy(urlCrawlScope);

    crawlerConfig.setIgnoreCanonicalLinks(true);
    crawlerConfig.setFetchHttpHead(true);
    crawlerConfig.setIgnoreSitemap(true);

    try {
      var manager = new Manager(PER_HOST_CRAWLING_LIMIT, this.datasource, this.startUrls.stream());
      try (JdbcStoreEngine engine = new JdbcStoreEngine(manager)) {

        engine.setConfigProperties(Main.dbProps());
        crawlerConfig.setDataStoreEngine(engine);

        var htmlLinkExtractor = new HtmlLinkExtractor();
        htmlLinkExtractor.setIgnoreLinkData(true);
        htmlLinkExtractor.removeLinkTag("img", "src");

        crawlerConfig.setLinkExtractors(htmlLinkExtractor);

        //        crawlerConfig.setReferenceFilters(manager);
        crawlerConfig.setEventListeners(manager);
        crawlerConfig.setMetadataFilters(manager);
        //        crawlerConfig.setHttpFetchers(manager.httpFetcher());
        crawlerConfig.setDelayResolver(manager.delayResolver());

        crawlerConfig.setCommitters(new TantivyCommitter(this.client, manager));

        config.setCrawlerConfigs(crawlerConfig);

        ProCollector spider = new ProCollector(config, manager);

        if (Boolean.parseBoolean(System.getProperty("clean-crawler-data", "false"))) {
          spider.clean();
        }

        spider.start();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
