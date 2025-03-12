package com.milindmantri;

import com.norconex.collector.core.doc.CrawlDocInfo;
import com.norconex.collector.core.pipeline.DocInfoPipelineContext;
import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.crawler.HttpCrawler;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.doc.HttpDocInfo;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipeline;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipelineContext;
import com.norconex.collector.http.pipeline.queue.HttpQueuePipeline;
import com.norconex.collector.http.pipeline.queue.HttpQueuePipelineContext;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import com.norconex.importer.response.ImporterResponse;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

public class ProCrawler extends HttpCrawler {

  private final Manager manager;
  private final HttpCrawlerConfig config;
  private final IPipelineStage<DocInfoPipelineContext> countInitStage;
  private boolean isResuming;
  private boolean isRecrawl;

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
      final HttpCrawlerConfig crawlerConfig, final HttpCollector collector, final Manager manager) {
    super(crawlerConfig, collector);

    this.manager = manager;
    this.config = crawlerConfig;
    this.countInitStage =
        ctx -> this.manager.initCount(new Host(URI.create(ctx.getDocInfo().getReference())));
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
  protected void executeQueuePipeline(final CrawlDocInfo crawlRef) {

    HttpDocInfo httpData = (HttpDocInfo) crawlRef;
    HttpQueuePipelineContext context = new HttpQueuePipelineContext(this, httpData);

    var pipeline = new HttpQueuePipeline();
    List<IPipelineStage<DocInfoPipelineContext>> existingStages =
        pipeline.getStages().stream().toList();

    pipeline.clearStages();

    pipeline.addStage(this.countInitStage);
    pipeline.addStages(existingStages);

    pipeline.execute(context);
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

    super.beforeCrawlerExecution(resume);

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

  public boolean isRecrawling() {
    return this.isRecrawl;
  }

  public boolean isResuming() {
    return this.isResuming;
  }
}
