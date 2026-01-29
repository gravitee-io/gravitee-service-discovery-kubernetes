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
package io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.helper;

import static org.assertj.core.api.Assertions.assertThat;

import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.KubernetesServiceDiscoveryServiceConfiguration;
import io.gravitee.definition.model.v4.endpointgroup.Endpoint;
import io.gravitee.definition.model.v4.endpointgroup.EndpointGroup;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointCriteria;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointManager;
import io.gravitee.gateway.reactive.core.v4.endpoint.ManagedEndpoint;
import io.gravitee.gateway.reactive.handlers.api.v4.Api;
import io.gravitee.kubernetes.client.model.v1.EndpointSlice;
import io.gravitee.kubernetes.client.model.v1.EndpointSliceConditions;
import io.gravitee.kubernetes.client.model.v1.EndpointSliceEndpoint;
import io.gravitee.kubernetes.client.model.v1.EndpointSlicePort;
import io.gravitee.kubernetes.client.model.v1.Event;
import io.gravitee.kubernetes.client.model.v1.KubernetesEventType;
import io.gravitee.kubernetes.client.model.v1.ObjectMeta;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KubernetesEventHandlerTest {

  private static final String GROUP_NAME = "default";
  private static final String GROUP_TYPE = "http-proxy";
  private static final String DEFAULT_IP = "10.0.0.1";
  private static final String SECOND_IP = "10.0.0.2";
  private static final int DEFAULT_PORT = 8080;
  private static final AtomicInteger SLICE_SEQ = new AtomicInteger();

  private final Api api = new Api(
    io.gravitee.definition.model.v4.Api.builder().name("my-api").build()
  );

  @Test
  void should_add_endpoints_from_snapshot() {
    RecordingEndpointManager manager = new RecordingEndpointManager();
    KubernetesServiceDiscoveryServiceConfiguration config =
      KubernetesServiceDiscoveryServiceConfiguration.builder()
        .port(DEFAULT_PORT)
        .build();

    KubernetesEventHandler handler = createHandler(manager, config);
    handler.handleSnapshot(List.of(endpointSlice(DEFAULT_IP, DEFAULT_PORT)));

    assertThat(manager.endpoints).containsKey(
      endpointName(DEFAULT_IP, DEFAULT_PORT)
    );
  }

  @Test
  void should_remove_endpoints_on_delete_event() throws InterruptedException {
    RecordingEndpointManager manager = new RecordingEndpointManager();
    KubernetesServiceDiscoveryServiceConfiguration config =
      KubernetesServiceDiscoveryServiceConfiguration.builder()
        .port(DEFAULT_PORT)
        .build();

    KubernetesEventHandler handler = createHandler(manager, config);
    EndpointSlice slice1 = endpointSlice(DEFAULT_IP, DEFAULT_PORT);
    EndpointSlice slice2 = endpointSlice(SECOND_IP, DEFAULT_PORT);
    handler.handleSnapshot(List.of(slice1, slice2));

    handler.handle(new Event<>(KubernetesEventType.DELETED.name(), slice1));

    Thread.sleep(700);

    assertThat(manager.disabled).isEmpty();
    assertThat(manager.removed).isEmpty();
    assertThat(manager.endpoints).containsKey(
      endpointName(DEFAULT_IP, DEFAULT_PORT)
    );
  }

  @Test
  void should_remove_endpoints_on_modified_event_with_removed_addresses()
    throws Exception {
    RecordingEndpointManager manager = new RecordingEndpointManager();
    KubernetesServiceDiscoveryServiceConfiguration config =
      KubernetesServiceDiscoveryServiceConfiguration.builder()
        .port(DEFAULT_PORT)
        .build();

    KubernetesEventHandler handler = createHandler(manager, config);
    EndpointSlice slice1 = endpointSlice(DEFAULT_IP, DEFAULT_PORT);
    EndpointSlice slice2 = endpointSlice(SECOND_IP, DEFAULT_PORT);
    handler.handleSnapshot(List.of(slice1, slice2));

    EndpointSlice updated = endpointSlice(SECOND_IP, DEFAULT_PORT);
    handler.handle(new Event<>(KubernetesEventType.MODIFIED.name(), updated));

    Thread.sleep(700);

    assertThat(manager.disabled).contains(
      endpointName(DEFAULT_IP, DEFAULT_PORT)
    );
    assertThat(manager.removed).contains(
      endpointName(DEFAULT_IP, DEFAULT_PORT)
    );
    assertThat(manager.endpoints).containsKey(
      endpointName(SECOND_IP, DEFAULT_PORT)
    );
  }

  @Test
  void should_remove_endpoints_when_becoming_not_ready() throws Exception {
    RecordingEndpointManager manager = new RecordingEndpointManager();
    KubernetesServiceDiscoveryServiceConfiguration config =
      KubernetesServiceDiscoveryServiceConfiguration.builder()
        .port(DEFAULT_PORT)
        .build();

    KubernetesEventHandler handler = createHandler(manager, config);
    EndpointSlice slice = endpointSlice(DEFAULT_IP, DEFAULT_PORT);
    handler.handleSnapshot(List.of(slice));

    EndpointSlice notReady = endpointSliceNotReady(DEFAULT_IP, DEFAULT_PORT);
    handler.handle(new Event<>(KubernetesEventType.MODIFIED.name(), notReady));

    Thread.sleep(700);

    assertThat(manager.removed).contains(
      endpointName(DEFAULT_IP, DEFAULT_PORT)
    );
    assertThat(manager.endpoints).containsKey(
      endpointName(SECOND_IP, DEFAULT_PORT)
    );
  }

  @Test
  void should_keep_last_known_endpoints_when_all_not_ready() throws Exception {
    RecordingEndpointManager manager = new RecordingEndpointManager();
    KubernetesServiceDiscoveryServiceConfiguration config =
      KubernetesServiceDiscoveryServiceConfiguration.builder()
        .port(DEFAULT_PORT)
        .build();

    KubernetesEventHandler handler = createHandler(manager, config);
    EndpointSlice slice = endpointSlice(DEFAULT_IP, DEFAULT_PORT);
    handler.handleSnapshot(List.of(slice));

    EndpointSlice notReady = endpointSliceNotReady(DEFAULT_IP, DEFAULT_PORT);
    handler.handle(new Event<>(KubernetesEventType.MODIFIED.name(), notReady));

    Thread.sleep(700);

    assertThat(manager.disabled).isEmpty();
    assertThat(manager.removed).isEmpty();
    assertThat(manager.endpoints).containsKey(
      endpointName(DEFAULT_IP, DEFAULT_PORT)
    );
  }

  @Test
  void should_add_all_ports_when_no_port_configured() {
    RecordingEndpointManager manager = new RecordingEndpointManager();
    KubernetesServiceDiscoveryServiceConfiguration config =
      new KubernetesServiceDiscoveryServiceConfiguration();

    KubernetesEventHandler handler = createHandler(manager, config);
    handler.handleSnapshot(
      List.of(endpointSlice(DEFAULT_IP, DEFAULT_PORT, 9090))
    );

    assertThat(manager.endpoints)
      .containsKey(endpointName(DEFAULT_IP, DEFAULT_PORT))
      .containsKey(endpointName(DEFAULT_IP, 9090));
  }

  private KubernetesEventHandler createHandler(
    RecordingEndpointManager manager,
    KubernetesServiceDiscoveryServiceConfiguration config
  ) {
    EndpointGroup group = EndpointGroup.builder()
      .name(GROUP_NAME)
      .type(GROUP_TYPE)
      .build();
    return new KubernetesEventHandler(
      api,
      manager,
      group,
      config,
      0,
      new HashMap<>()
    );
  }

  private static String endpointName(String ip, int port) {
    return "kubernetes#" + ip + "#" + port;
  }

  private static EndpointSlice endpointSlice(String ip, int... ports) {
    EndpointSliceEndpoint endpoint = new EndpointSliceEndpoint();
    endpoint.setAddresses(List.of(ip));

    List<EndpointSlicePort> endpointPorts = new ArrayList<>();
    for (int port : ports) {
      EndpointSlicePort endpointPort = new EndpointSlicePort();
      endpointPort.setPort(port);
      endpointPorts.add(endpointPort);
    }

    EndpointSlice slice = new EndpointSlice();
    slice.setEndpoints(List.of(endpoint));
    slice.setPorts(endpointPorts);
    slice.setMetadata(newSliceMeta());
    return slice;
  }

  private static EndpointSlice endpointSliceNotReady(String ip, int... ports) {
    EndpointSliceConditions conditions = new EndpointSliceConditions();
    conditions.setReady(false);

    EndpointSliceEndpoint endpoint = new EndpointSliceEndpoint();
    endpoint.setAddresses(List.of(ip));
    endpoint.setConditions(conditions);

    List<EndpointSlicePort> endpointPorts = new ArrayList<>();
    for (int port : ports) {
      EndpointSlicePort endpointPort = new EndpointSlicePort();
      endpointPort.setPort(port);
      endpointPorts.add(endpointPort);
    }

    EndpointSlice slice = new EndpointSlice();
    slice.setEndpoints(List.of(endpoint));
    slice.setPorts(endpointPorts);
    slice.setMetadata(newSliceMeta());
    return slice;
  }

  private static ObjectMeta newSliceMeta() {
    ObjectMeta meta = new ObjectMeta();
    meta.setName("slice-" + SLICE_SEQ.incrementAndGet());
    return meta;
  }

  private static class RecordingEndpointManager implements EndpointManager {

    private final Map<String, Endpoint> endpoints = new HashMap<>();
    private final Set<String> disabled = new HashSet<>();
    private final Set<String> removed = new HashSet<>();
    private io.gravitee.common.component.Lifecycle.State state =
      io.gravitee.common.component.Lifecycle.State.STARTED;

    @Override
    public void addOrUpdateEndpoint(String groupName, Endpoint endpoint) {
      endpoints.put(endpoint.getName(), endpoint);
    }

    @Override
    public void removeEndpoint(String name) {
      removed.add(name);
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
    public void disable(ManagedEndpoint endpoint) {
      if (endpoint != null) {
        disabled.add(endpoint.getDefinition().getName());
      }
    }

    @Override
    public void disable(String name) {
      disabled.add(name);
    }

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
