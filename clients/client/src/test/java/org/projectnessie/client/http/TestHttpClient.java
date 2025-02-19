/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.projectnessie.client.util.TestHttpUtil;
import org.projectnessie.client.util.TestServer;
import org.projectnessie.model.CommitMeta;

@Execution(ExecutionMode.CONCURRENT)
public class TestHttpClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();

  private static HttpRequest get(InetSocketAddress address) {
    return get(address, false);
  }

  private static HttpRequest get(InetSocketAddress address, boolean disableCompression) {
    return get(address, 15000, 15000, disableCompression);
  }

  private static HttpRequest get(
      InetSocketAddress address, int connectTimeout, int readTimeout, boolean disableCompression) {
    return HttpClient.builder()
        .setBaseUri(URI.create("http://localhost:" + address.getPort()))
        .setObjectMapper(MAPPER)
        .setConnectionTimeoutMillis(connectTimeout)
        .setReadTimeoutMillis(readTimeout)
        .setDisableCompression(disableCompression)
        .build()
        .newRequest();
  }

  @Test
  void testGet() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("GET", h.getRequestMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          TestHttpUtil.writeResponseBody(h, response);
        };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean = get(server.getAddress()).get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testReadTimeout() {
    HttpHandler handler = h -> {};
    Assertions.assertThrows(
        HttpClientReadTimeoutException.class,
        () -> {
          try (TestServer server = new TestServer(handler)) {
            get(server.getAddress(), 15000, 1, true).get().readEntity(ExampleBean.class);
          }
        });
  }

  @Test
  void testPut() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("PUT", h.getRequestMethod());
          assertThat(h.getRequestHeaders())
              .containsEntry("Content-Encoding", Collections.singletonList("gzip"));
          try (InputStream in = new GZIPInputStream(h.getRequestBody())) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            Assertions.assertEquals(inputBean, bean);
          }
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress()).put(inputBean);
    }
  }

  @Test
  void testPost() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("POST", h.getRequestMethod());
          assertThat(h.getRequestHeaders())
              .containsEntry("Content-Encoding", Collections.singletonList("gzip"));
          try (InputStream in = new GZIPInputStream(h.getRequestBody())) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            Assertions.assertEquals(inputBean, bean);
          }
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress()).post(inputBean);
    }
  }

  @Test
  void testPutNoCompression() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("PUT", h.getRequestMethod());
          assertThat(h.getRequestHeaders()).doesNotContainKeys("Content-Encoding");
          try (InputStream in = h.getRequestBody()) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            Assertions.assertEquals(inputBean, bean);
          }
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress(), true).put(inputBean);
    }
  }

  @Test
  void testPostNoCompression() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("POST", h.getRequestMethod());
          assertThat(h.getRequestHeaders()).doesNotContainKeys("Content-Encoding");
          try (InputStream in = h.getRequestBody()) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            Assertions.assertEquals(inputBean, bean);
          }
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress(), true).post(inputBean);
    }
  }

  @Test
  void testDelete() throws Exception {
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("DELETE", h.getRequestMethod());
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress()).delete();
    }
  }

  @Test
  void testGetQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("x=y", h.getRequestURI().getQuery());
          Assertions.assertEquals("GET", h.getRequestMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          TestHttpUtil.writeResponseBody(h, response);
        };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean =
          get(server.getAddress()).queryParam("x", "y").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetMultipleQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          String[] queryParams = h.getRequestURI().getQuery().split("&");
          Assertions.assertEquals(2, queryParams.length);
          Set<String> queryParamSet = new HashSet<>(Arrays.asList(queryParams));
          Assertions.assertTrue(queryParamSet.contains("x=y"));
          Assertions.assertTrue(queryParamSet.contains("a=b"));
          Assertions.assertEquals("GET", h.getRequestMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          TestHttpUtil.writeResponseBody(h, response);
        };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean =
          get(server.getAddress())
              .queryParam("x", "y")
              .queryParam("a", "b")
              .get()
              .readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetNullQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          String queryParams = h.getRequestURI().getQuery();
          Assertions.assertNull(queryParams);
          Assertions.assertEquals("GET", h.getRequestMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          TestHttpUtil.writeResponseBody(h, response);
        };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean =
          get(server.getAddress())
              .queryParam("x", (String) null)
              .get()
              .readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetTemplate() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpHandler handler =
        h -> {
          Assertions.assertEquals("GET", h.getRequestMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          TestHttpUtil.writeResponseBody(h, response);
        };
    try (TestServer server = new TestServer("/a/b", handler)) {
      ExampleBean bean = get(server.getAddress()).path("a/b").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
      bean =
          get(server.getAddress())
              .path("a/{b}")
              .resolveTemplate("b", "b")
              .get()
              .readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetTemplateThrows() throws Exception {
    HttpHandler handler = h -> Assertions.fail();
    try (TestServer server = new TestServer("/a/b", handler)) {
      Assertions.assertThrows(
          HttpClientException.class,
          () -> get(server.getAddress()).path("a/{b}").get().readEntity(ExampleBean.class));
      Assertions.assertThrows(
          HttpClientException.class,
          () ->
              get(server.getAddress())
                  .path("a/b")
                  .resolveTemplate("b", "b")
                  .get()
                  .readEntity(ExampleBean.class));
    }
  }

  @Test
  void testFilters() throws Exception {
    AtomicBoolean requestFilterCalled = new AtomicBoolean(false);
    AtomicBoolean responseFilterCalled = new AtomicBoolean(false);
    AtomicReference<ResponseContext> responseContextGotCallback = new AtomicReference<>();
    AtomicReference<ResponseContext> responseContextGotFilter = new AtomicReference<>();
    HttpHandler handler =
        h -> {
          Assertions.assertTrue(h.getRequestHeaders().containsKey("x"));
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      HttpClient.Builder client =
          HttpClient.builder()
              .setBaseUri(URI.create("http://localhost:" + server.getAddress().getPort()))
              .setObjectMapper(MAPPER);
      client.addRequestFilter(
          context -> {
            requestFilterCalled.set(true);
            context.putHeader("x", "y");
            context.addResponseCallback(
                (responseContext, failure) -> {
                  responseContextGotCallback.set(responseContext);
                  Assertions.assertNull(failure);
                });
          });
      client.addResponseFilter(
          con -> {
            try {
              Assertions.assertEquals(Status.OK, con.getResponseCode());
              responseFilterCalled.set(true);
              responseContextGotFilter.set(con);
            } catch (IOException e) {
              throw new IOError(e);
            }
          });
      client.build().newRequest().get();
      Assertions.assertNotNull(responseContextGotFilter.get());
      Assertions.assertSame(responseContextGotFilter.get(), responseContextGotCallback.get());
      Assertions.assertTrue(responseFilterCalled.get());
      Assertions.assertTrue(requestFilterCalled.get());
    }
  }

  @Test
  void testHeaders() throws Exception {
    HttpHandler handler =
        h -> {
          Assertions.assertTrue(h.getRequestHeaders().containsKey("x"));
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress()).header("x", "y").get();
    }
  }

  @Test
  void testMultiValueHeaders() throws Exception {
    HttpHandler handler =
        h -> {
          Assertions.assertTrue(h.getRequestHeaders().containsKey("x"));
          List<String> values = h.getRequestHeaders().get("x");
          Assertions.assertEquals(2, values.size());
          Assertions.assertEquals("y", values.get(0));
          Assertions.assertEquals("z", values.get(1));
          h.sendResponseHeaders(200, 0);
        };
    try (TestServer server = new TestServer(handler)) {
      get(server.getAddress()).header("x", "y").header("x", "z").get();
    }
  }

  public static class ExampleBean {
    private String field1;
    private int field2;
    private Instant field3;

    public ExampleBean() {}

    /** all-args constructor. */
    public ExampleBean(String field1, int field2, Instant field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }

    public String getField1() {
      return field1;
    }

    public int getField2() {
      return field2;
    }

    public ExampleBean setField1(String field1) {
      this.field1 = field1;
      return this;
    }

    public ExampleBean setField2(int field2) {
      this.field2 = field2;
      return this;
    }

    @JsonSerialize(using = CommitMeta.InstantSerializer.class)
    @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
    public Instant getField3() {
      return field3;
    }

    public void setField3(Instant field3) {
      this.field3 = field3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ExampleBean)) {
        return false;
      }
      ExampleBean that = (ExampleBean) o;
      return field2 == that.field2
          && Objects.equals(field1, that.field1)
          && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }
  }
}
