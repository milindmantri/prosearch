package com.milindmantri;

import com.norconex.collector.core.pipeline.DocInfoPipelineContext;
import com.norconex.collector.http.doc.HttpDocInfo;
import com.norconex.collector.http.pipeline.queue.HttpQueuePipeline;
import com.norconex.collector.http.pipeline.queue.HttpQueuePipelineContext;
import com.norconex.collector.http.robot.RobotsTxt;
import com.norconex.collector.http.sitemap.ISitemapResolver;
import com.norconex.commons.lang.pipeline.IPipelineStage;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/** Useful to start up crawler quickly by enabling async resolution of start URLs and sitemaps */
record FastQueuePipeline(SemaphoredExecutor ex) implements IPipelineStage<DocInfoPipelineContext> {

  private class AsyncQueuePipeline implements Callable<Boolean> {
    private final HttpQueuePipelineContext ctx;
    private final HttpDocInfo doc;

    AsyncQueuePipeline(final HttpQueuePipelineContext ctx, final HttpDocInfo doc) {
      this.ctx = ctx;
      this.doc = doc;
    }

    @Override
    public Boolean call() {

      HttpQueuePipelineContext context = new HttpQueuePipelineContext(ctx.getCrawler(), doc);
      var pipeline = new HttpQueuePipeline();

      var modifiedStages =
          pipeline.getStages().stream()
              .map(
                  stg ->
                      switch (stg.getClass().getSimpleName()) {
                        // Plug in our custom fast sitemap resolver
                        case "SitemapStage" -> FastQueuePipeline.this;
                        default -> stg;
                      })
              .toList();

      pipeline.clearStages();
      pipeline.addStages(modifiedStages);

      pipeline.execute(context);

      return true;
    }
  }

  @Override
  public boolean execute(final DocInfoPipelineContext c) {
    return switch (c) {
      case HttpQueuePipelineContext ctx -> resolveSitemap(ctx);
      default -> false;
    };
  }

  // TODO: Maybe this can be merged with execute()?
  public Future<?> queueStartUrl(final HttpQueuePipelineContext ctx) {
    return this.ex.submit(new AsyncQueuePipeline(ctx, ctx.getDocInfo()));
  }

  private boolean resolveSitemap(final HttpQueuePipelineContext ctx) {
    if (ctx.getConfig().isIgnoreSitemap() || ctx.getSitemapResolver() == null) {
      return true;
    }
    String urlRoot = ctx.getDocInfo().getUrlRoot();

    Thread.currentThread().setName(new Host(URI.create(urlRoot)).toString());

    List<String> robotsTxtLocations = new ArrayList<>();
    RobotsTxt robotsTxt = getRobotsTxt(ctx);
    if (robotsTxt != null) {
      robotsTxtLocations.addAll(robotsTxt.getSitemapLocations());
    }
    final ISitemapResolver sitemapResolver = ctx.getSitemapResolver();

    sitemapResolver.resolveSitemaps(
        ctx.getCrawler().getHttpFetchClient(),
        urlRoot,
        robotsTxtLocations,
        doc -> this.ex.submit(new AsyncQueuePipeline(ctx, doc)),
        false);

    return true;
  }

  // copied from HttpQueuePipeline
  private static RobotsTxt getRobotsTxt(HttpQueuePipelineContext ctx) {
    if (!ctx.getConfig().isIgnoreRobotsTxt()) {
      return ctx.getConfig()
          .getRobotsTxtProvider()
          .getRobotsTxt(ctx.getCrawler().getHttpFetchClient(), ctx.getDocInfo().getReference());
    }
    return null;
  }
}
