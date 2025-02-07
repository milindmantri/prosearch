package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

class TantivyClientTest {

  // Since HttpRequest equals does not compare POST body, using eq() arg matcher wouldn't compare
  // the JSON bodies of the requests. Using a little reflection in tests enables easy comparison
  class HttpRequestPostBody implements ArgumentMatcher<HttpRequest> {

    private final String content;
    private final HttpRequest httpRequest;

    public HttpRequestPostBody(final HttpRequest httpRequest) {
      this.httpRequest = httpRequest;
      this.content =
          httpRequest
              .bodyPublisher()
              .map(HttpRequestPostBody::accessContent)
              .orElseThrow(
                  () -> new IllegalArgumentException("httpRequest must have a body publisher."));
    }

    @Override
    public boolean matches(HttpRequest right) {

      return Objects.equals(this.httpRequest, right)
          && right
              .bodyPublisher()
              .map(HttpRequestPostBody::accessContent)
              .map(this.content::equals)
              .orElse(false);
    }

    private static String accessContent(final HttpRequest.BodyPublisher bodyPublisher) {
      try {
        Field f = bodyPublisher.getClass().getSuperclass().getDeclaredField("content");
        f.setAccessible(true);

        return new String((byte[]) f.get(bodyPublisher));
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  void delete() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.body()).thenReturn("true");
    when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);

    Mockito.when(httpClient.<String>send(any(), any())).thenReturn(response);

    assertTrue(tc.delete(URI.create("http://delete-this-link.com")));

    Mockito.verify(httpClient, times(1))
        .send(
            eq(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost/delete/?url=http://delete-this-link.com"))
                    .GET()
                    .build()),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void deleteResponseValid() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn("true");

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertTrue(tc.delete(URI.create("http://delete-this-link.com")));
  }

  @Test
  void deleteResponseInvalid() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    when(response.body()).thenReturn("error");

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertFalse(tc.delete(URI.create("http://delete-this-link.com")));
  }

  @Test
  void indexWithTitle() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    var httpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost/index/"))
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    """
                    {"url":"http://index-this-link.com"\
                    ,"title":"My \\"Quoted\\" Title"\
                    ,"body":"\\"Quoted\\" content to index"\
                    }\
                    """))
            .build();

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.body()).thenReturn("true");
    when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);

    Mockito.when(httpClient.<String>send(any(), any())).thenReturn(response);

    assertTrue(
        tc.index(
            URI.create("http://index-this-link.com"),
            "My \"Quoted\" Title",
            "\"Quoted\" content to index"));

    Mockito.verify(httpClient, times(1))
        .send(argThat(new HttpRequestPostBody(httpRequest)), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void indexWithTitleValidResponse() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn("true");

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertTrue(
        tc.index(
            URI.create("http://index-this-link.com"),
            "My \"Quoted\" Title",
            "\"Quoted\" content to index"));
  }

  @Test
  void indexWithTitleInvalidResponse() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    when(response.body()).thenReturn("Some error");

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertFalse(
        tc.index(
            URI.create("http://index-this-link.com"),
            "My \"Quoted\" Title",
            "\"Quoted\" content to index"));
  }

  @Test
  void indexWithoutTitle() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    var httpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost/index/"))
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    """
            {"url":"http://index-this-link.com"\
            ,"title":"http://index-this-link.com"\
            ,"body":"\\"Quoted\\" content to index"\
            }\
            """))
            .build();

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.body()).thenReturn("true");
    when(response.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);

    Mockito.when(httpClient.<String>send(any(), any())).thenReturn(response);

    assertTrue(tc.index(URI.create("http://index-this-link.com"), "\"Quoted\" content to index"));

    Mockito.verify(httpClient, times(1))
        .send(argThat(new HttpRequestPostBody(httpRequest)), any(HttpResponse.BodyHandler.class));
  }
}
