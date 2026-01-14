/*
 * Copyright © 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.KubernetesServiceDiscoveryServiceConfiguration;
import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.helper.KubernetesEventHandler;
import io.gravitee.definition.model.v4.endpointgroup.Endpoint;
import io.gravitee.definition.model.v4.endpointgroup.EndpointGroup;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointCriteria;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointManager;
import io.gravitee.gateway.reactive.core.v4.endpoint.ManagedEndpoint;
import io.gravitee.gateway.reactive.handlers.api.v4.Api;
import io.gravitee.kubernetes.client.KubernetesClient;
import io.gravitee.kubernetes.client.api.ResourceQuery;
import io.gravitee.kubernetes.client.config.KubernetesConfig;
import io.gravitee.kubernetes.client.impl.KubernetesClientV1Impl;
import io.gravitee.kubernetes.client.model.v1.Endpoints;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

class KubernetesEndpointDiscoveryIntegrationTest {

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String SERVICE_NAME = "my-service";
  private static final String ENDPOINT_IP = "10.0.0.1";
  private static final int ENDPOINT_PORT = 8080;
  private static final String GROUP_NAME = "default";
  private static final String GROUP_TYPE = "http-proxy";

  private final Api api = new Api(
    io.gravitee.definition.model.v4.Api.builder().name("my-api").build()
  );

  @StartStop
  public final MockWebServer server = new MockWebServer();

  @Test
  void should_resolve_endpoints_from_kubernetes_api() {
    server.enqueue(
      new MockResponse.Builder()
        .body(endpointsPayload())
        .addHeader("Content-Type", "application/json")
        .build()
    );

    KubernetesConfig config = KubernetesConfig.newInstance();
    config.setApiServerHost(server.getHostName());
    config.setApiServerPort(server.getPort());
    config.setUseSSL(false);
    config.setVerifyHost(false);

    KubernetesClient client = new KubernetesClientV1Impl(config);
    @SuppressWarnings("unchecked")
    ResourceQuery<Endpoints> query = (ResourceQuery<Endpoints>) (ResourceQuery<
      ?
    >) ResourceQuery.endpoints(DEFAULT_NAMESPACE, SERVICE_NAME).build();
    Endpoints endpoints = client.get(query).blockingGet();
    assertThat(endpoints).isNotNull();
    assertThat(endpoints.getSubsets()).isNotNull();

    EndpointGroup group = EndpointGroup.builder()
      .name(GROUP_NAME)
      .type(GROUP_TYPE)
      .build();
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();
    configuration.setPort(ENDPOINT_PORT);

    RecordingEndpointManager endpointManager = new RecordingEndpointManager();
    KubernetesEventHandler handler = new KubernetesEventHandler(
      api,
      endpointManager,
      group,
      configuration,
      0,
      new ConcurrentHashMap<>()
    );
    handler.handleSnapshot(List.of(endpoints));

    assertThat(endpointManager.endpoints).containsKey(
      endpointName(ENDPOINT_IP, ENDPOINT_PORT)
    );
  }

  private static String endpointsPayload() {
    return """
    {
      "metadata": {"name": "my-service"},
      "subsets": [
        {
          "addresses": [{"ip": "10.0.0.1"}],
          "ports": [{"port": 8080}]
        }
      ]
    }
    """;
  }

  private static String endpointName(String ip, int port) {
    return "kubernetes#" + ip + "#" + port;
  }

  private static class RecordingEndpointManager implements EndpointManager {

    private final Map<String, Endpoint> endpoints = new HashMap<>();
    private io.gravitee.common.component.Lifecycle.State state =
      io.gravitee.common.component.Lifecycle.State.STARTED;

    @Override
    public void addOrUpdateEndpoint(String groupName, Endpoint endpoint) {
      endpoints.put(endpoint.getName(), endpoint);
    }

    @Override
    public void removeEndpoint(String name) {
      endpoints.remove(name);
    }

    @Override
    public ManagedEndpoint next() {
      return null;
    }

    @Override
    public List<ManagedEndpoint> all() {
      return List.of();
    }

    @Override
    public String addListener(
      java.util.function.BiConsumer<
        EndpointManager.Event,
        ManagedEndpoint
      > endpointConsumer
    ) {
      return "listener";
    }

    @Override
    public void removeListener(String listenerId) {}

    @Override
    public ManagedEndpoint next(EndpointCriteria criteria) {
      return null;
    }

    @Override
    public void disable(ManagedEndpoint endpoint) {}

    @Override
    public void disable(String name) {}

    @Override
    public void enable(ManagedEndpoint endpoint) {}

    @Override
    public io.gravitee.common.component.Lifecycle.State lifecycleState() {
      return state;
    }

    @Override
    public EndpointManager start() {
      state = io.gravitee.common.component.Lifecycle.State.STARTED;
      return this;
    }

    @Override
    public EndpointManager stop() {
      state = io.gravitee.common.component.Lifecycle.State.STOPPED;
      return this;
    }
  }
}
