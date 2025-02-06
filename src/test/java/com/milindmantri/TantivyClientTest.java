package com.milindmantri;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TantivyClientTest {

  @Test
  void delete() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    tc.delete(URI.create("http://delete-this-link.com"));

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
  void deleteResponeValid() throws IOException, InterruptedException {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    URI host = URI.create("http://localhost");
    var tc = new TantivyClient(httpClient, host);

    HttpResponse<String> response = Mockito.mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn("true");

    // httpclient should receive correct URI, json body, json header and GET
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertTrue(tc.delete(URI.create("http://delete-this-link.com")));
  }
}
