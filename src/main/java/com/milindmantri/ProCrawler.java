package com.milindmantri;

import com.norconex.collector.core.pipeline.importer.ImporterPipelineContext;
import com.norconex.collector.http.HttpCollector;
import com.norconex.collector.http.crawler.HttpCrawler;
import com.norconex.collector.http.crawler.HttpCrawlerConfig;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipeline;
import com.norconex.collector.http.pipeline.importer.HttpImporterPipelineContext;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import com.norconex.importer.response.ImporterResponse;
import java.util.List;

public class ProCrawler extends HttpCrawler {

  private final DomainCounter domainCounter;

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
      final DomainCounter domainCounter) {
    super(crawlerConfig, collector);

    this.domainCounter = domainCounter;
  }

  @Override
  protected ImporterResponse executeImporterPipeline(
      final ImporterPipelineContext importerContext) {
    HttpImporterPipelineContext httpContext = new HttpImporterPipelineContext(importerContext);

    new ImporterQueueRejectPipeline(
            getCrawlerConfig().isKeepDownloads(),
            importerContext.getDocument().isOrphan(),
            domainCounter)
        .execute(httpContext);

    return httpContext.getImporterResponse();
  }
}
