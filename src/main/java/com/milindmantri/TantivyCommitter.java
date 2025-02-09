package com.milindmantri;

import com.norconex.committer.core3.AbstractCommitter;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.xml.XML;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.stream.Collectors;

public class TantivyCommitter extends AbstractCommitter {

  private final TantivyClient client;

  public TantivyCommitter(final TantivyClient client) {
    if (client == null) {
      throw new IllegalArgumentException("client must not be null.");
    }
    this.client = client;
  }

  @Override
  protected void doInit() throws CommitterException {}

  @Override
  protected void doUpsert(final UpsertRequest upsertRequest) throws CommitterException {
    if (upsertRequest == null) {
      throw new IllegalArgumentException("upsertRequest must not be null.");
    }

    // TODO: Add stats for indexed pages per domain

    try {
      boolean deleteResult = client.delete(URI.create(upsertRequest.getReference()));

      boolean indexResult =
          upsertRequest.getMetadata().containsKey("title")
              ? client.index(
                  URI.create(upsertRequest.getReference()),
                  upsertRequest.getMetadata().getString("title"),
                  new BufferedReader(new InputStreamReader(upsertRequest.getContent()))
                      .lines()
                      .collect(Collectors.joining()))
              : client.index(
                  URI.create(upsertRequest.getReference()),
                  new BufferedReader(new InputStreamReader(upsertRequest.getContent()))
                      .lines()
                      .collect(Collectors.joining()));

      if (deleteResult && indexResult) {
        // TODO: Insert into DB
      } else {
        throw new CommitterException(
            String.format(
                "Upsert failed for request, %s, because delete op was %s and index op was %s",
                upsertRequest, deleteResult, indexResult));
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doDelete(final DeleteRequest deleteRequest) throws CommitterException {
    if (deleteRequest == null) {
      throw new IllegalArgumentException("deleteRequest must not be null.");
    }

    try {
      if (!client.delete(URI.create(deleteRequest.getReference()))) {
        throw new CommitterException(
            "Could not process delete request for %s".formatted(deleteRequest));
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doClose() throws CommitterException {
    // TODO: Implement
  }

  @Override
  protected void doClean() throws CommitterException {
    // TODO: Implement
  }

  @Override
  public void loadCommitterFromXML(final XML xml) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public void saveCommitterToXML(final XML xml) {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
