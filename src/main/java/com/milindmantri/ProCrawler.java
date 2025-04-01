package com.milindmantri;

import com.norconex.collector.core.monitor.MdcUtil;
import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.crawler.HttpCrawler;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.doc.HttpDocInfo;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipeline;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipelineContext;
import com.norconex.collector.http.pipeline.queue.HttpQueuePipelineContext;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import com.norconex.importer.response.ImporterResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProCrawler extends HttpCrawler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProCrawler.class);

  private final Manager manager;
  private final SemaphoredExecutor exec;
  private boolean isResuming;
  private boolean isRecrawl;
  private boolean isFastQueueInit;

  private static class ImporterQueueRejectPipeline extends HttpImporterPipeline {

    public ImporterQueueRejectPipeline(
        final boolean isKeepDownloads,
        final boolean isOrphan,
        final IPipelineStage<ImporterPipelineContext> domainCounter) {
      super(isKeepDownloads, isOrphan);

      List<IPipelineStage<ImporterPipelineContext>> stages = getStages().stream().toList();
      this.clearStages();

      // first stage
      this.addStage(domainCounter);
      this.addStages(stages);
    }
  }

  public ProCrawler(
      final HttpCrawlerConfig crawlerConfig,
      final HttpCollector collector,
      final Manager manager,
      final SemaphoredExecutor exec) {
    super(crawlerConfig, collector);

    this.manager = manager;
    this.exec = exec;
  }

  @Override
  protected ImporterResponse executeImporterPipeline(
      final ImporterPipelineContext importerContext) {
    HttpImporterPipelineContext httpContext = new HttpImporterPipelineContext(importerContext);

    new ImporterQueueRejectPipeline(
            getCrawlerConfig().isKeepDownloads(), importerContext.getDocument().isOrphan(), manager)
        .execute(httpContext);

    return httpContext.getImporterResponse();
  }

  @Override
  protected void beforeCrawlerExecution(final boolean resume) {
    // init is guaranteed to be called by the lib

    this.manager.setCrawler(this);
    this.isResuming = resume;

    // get from cached store
    var isCacheEmpty = ((JdbcStoreEngine) this.getDataStoreEngine()).isCacheEmpty();
    if (!isCacheEmpty) {
      this.isRecrawl = true;
    }

    // take the queueing start urls mechanism in our hands, passing true never runs the queuing
    // work, but still inits the other essentials.
    super.beforeCrawlerExecution(true);

    if (!resume) {
      crawlStartUrlsAndSitemapFast(new FastQueuePipeline(exec));
    }

    this.isFastQueueInit = true;

    LOGGER.info("Crawling start URLs is complete.");

    // initial queue is not set correctly if the crawler fails when doing initial crawl
    // TODO: Improve by finding diff-ed start URLs and initiating a crawl on them

    try {

      if (Boolean.parseBoolean(System.getProperty("clean-crawler-data", "false"))) {
        this.manager.dropStatsTable();
      }

      this.manager.createStatsTableIfNotExists();

      manager.restoreCount();

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This crawls all start URLs concurrently which in turn calls the fast queue pipeline, enabling
   * faster initialization of the queue allowing crawling to begin sooner.
   */
  private void crawlStartUrlsAndSitemapFast(final FastQueuePipeline fast) {
    List<String> startURLs = getCrawlerConfig().getStartURLs();
    MdcUtil.setCrawlerId(getId());
    Thread.currentThread().setName(getId());

    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {

      startURLs.stream()
          .filter(StringUtils::isNotBlank)
          .map(s -> new HttpQueuePipelineContext(this, new HttpDocInfo(s, 0)))
          .map(fast::queueStartUrl)
          .<Callable<?>>map(f -> (f::get))
          .forEach(scope::fork);

      scope.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean isQueueInitialized() {
    return this.isFastQueueInit;
  }

  public boolean isRecrawling() {
    return this.isRecrawl;
  }

  public boolean isResuming() {
    return this.isResuming;
  }
}
