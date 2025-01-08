package com.milindmantri;

import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.HttpCollectorConfig;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.crawler.URLCrawlScopeStrategy;

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

    // TODO: What about the case for redirected domains, like openjdk.java.net
    var urlCrawlScope = new URLCrawlScopeStrategy();
    urlCrawlScope.setStayOnDomain(true);
    urlCrawlScope.setIncludeSubdomains(false);

    crawlerConfig.setUrlCrawlScopeStrategy(urlCrawlScope);

    config.setCrawlerConfigs(crawlerConfig);

    HttpCollector spider = new HttpCollector(config);

    spider.start();
  }
}