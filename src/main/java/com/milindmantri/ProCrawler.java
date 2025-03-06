package com.milindmantri;

import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.crawler.HttpCrawler;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipeline;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipelineContext;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import com.norconex.importer.response.ImporterResponse;
import java.sql.SQLException;
import java.util.List;

public class ProCrawler extends HttpCrawler {

  private final Manager manager;
  private final HttpCrawlerConfig config;

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
      final Manager manager) {
    super(crawlerConfig, collector);

    this.manager = manager;
    this.config = crawlerConfig;
  }

  @Override
  protected ImporterResponse executeImporterPipeline(
      final ImporterPipelineContext importerContext) {
    HttpImporterPipelineContext httpContext = new HttpImporterPipelineContext(importerContext);

    new ImporterQueueRejectPipeline(
            getCrawlerConfig().isKeepDownloads(),
            importerContext.getDocument().isOrphan(),
      manager)
        .execute(httpContext);

    return httpContext.getImporterResponse();
  }

  @Override
  protected void beforeCrawlerExecution(final boolean resume) {
    super.beforeCrawlerExecution(resume);

    // initial queue is not set correctly if the crawler fails when doing initial crawl
    // TODO: Improve by finding diff-ed start URLs and initiating a crawl on them

    try {
      manager.restoreCount((JdbcStoreEngine) this.config.getDataStoreEngine());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
