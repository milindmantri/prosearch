package com.milindmantri;

import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;

public class Main {

  public static void main(String[] args) {

    // Why even allow for new HttpCollector(), when setting id is required. It will anyway error.
    HttpCollectorConfig config = new HttpCollectorConfig();
    config.setId("test");

    config.setMaxConcurrentCrawlers(2);

    HttpCrawlerConfig crawlerConfig = new HttpCrawlerConfig();

    // TODO: pass list of all URLs to crawl
    crawlerConfig.setStartURLs("https://www.php.net");
    crawlerConfig.setId("test-crawler");

    config.setCrawlerConfigs(crawlerConfig);

    HttpCollector spider = new HttpCollector(config);

    spider.start();
  }
}