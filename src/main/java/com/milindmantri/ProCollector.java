package com.milindmantri;

import com.norconex.collector.core.crawler.Crawler;
import com.norconex.collector.core.crawler.CrawlerConfig;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;

public class ProCollector extends HttpCollector {

  private final Manager manager;

  public ProCollector(final HttpCollectorConfig collectorConfig, final Manager manager) {
    super(collectorConfig);
    this.manager = manager;
  }

  @Override
  protected Crawler createCrawler(final CrawlerConfig config) {
    return new ProCrawler((HttpCrawlerConfig) config, this, this.manager);
  }
}
