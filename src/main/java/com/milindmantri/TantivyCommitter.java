package com.milindmantri;

import com.norconex.committer.core3.AbstractCommitter;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.xml.XML;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class TantivyCommitter extends AbstractCommitter {

  private final TantivyClient client;
  private final DataSource datasource;

  public TantivyCommitter(final TantivyClient client, final DataSource datasource) {
    if (client == null) {
      throw new IllegalArgumentException("client must not be null.");
    }

    if (datasource == null) {
      throw new IllegalArgumentException("datasource must not be null.");
    }

    this.client = client;
    this.datasource = datasource;
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
      final URI uri = URI.create(upsertRequest.getReference());
      boolean deleteResult = client.delete(uri);

      Optional<Long> maybeIndexedBytesLength =
          upsertRequest.getMetadata().containsKey("title")
              ? client.indexAndLength(
                  uri,
                  upsertRequest.getMetadata().getString("title"),
                  inputStreamReader(upsertRequest.getContent()))
              : client.indexAndLength(uri, inputStreamReader(upsertRequest.getContent()));

      if (deleteResult && maybeIndexedBytesLength.isPresent()) {
        deleteFromDomainStats(uri);
        insertIntoDomainStats(uri, maybeIndexedBytesLength.get());
      } else {
        throw new CommitterException(
            String.format(
                "Upsert failed for request, %s, because delete op was %s and index op was %s",
                upsertRequest, deleteResult, maybeIndexedBytesLength));
      }
    } catch (IOException | InterruptedException | SQLException e) {
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

  private static String inputStreamReader(final InputStream is) {
    return new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining());
  }

  private void insertIntoDomainStats(final URI uri, final long length) throws SQLException {
    try (var con = this.datasource.getConnection();
        var ps =
            con.prepareStatement("INSERT INTO domain_stats (host, url, length) VALUES (?, ?, ?)")) {
      ps.setString(
          1,
          "%s://%s"
              .formatted(
                  Objects.requireNonNull(uri.getScheme()),
                  Objects.requireNonNull(uri.getAuthority())));

      ps.setString(2, uri.toString());
      ps.setLong(3, length);
      ps.executeUpdate();
    }
  }

  private void deleteFromDomainStats(final URI uri) throws SQLException {
    try (var con = this.datasource.getConnection();
        var ps = con.prepareStatement("DELETE FROM domain_stats WHERE host = ? AND url = ?")) {
      ps.setString(
          1,
          "%s://%s"
              .formatted(
                  Objects.requireNonNull(uri.getScheme()),
                  Objects.requireNonNull(uri.getAuthority())));

      ps.setString(2, uri.toString());
      ps.executeUpdate();
    }
  }
}
