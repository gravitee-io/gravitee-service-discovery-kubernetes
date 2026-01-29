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

import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.KubernetesServiceDiscoveryServiceConfiguration;
import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.factory.EndpointFactory;
import io.gravitee.definition.model.v4.endpointgroup.EndpointGroup;
import io.gravitee.gateway.reactive.core.v4.endpoint.EndpointManager;
import io.gravitee.gateway.reactive.handlers.api.v4.Api;
import io.gravitee.kubernetes.client.model.v1.EndpointAddress;
import io.gravitee.kubernetes.client.model.v1.EndpointSlice;
import io.gravitee.kubernetes.client.model.v1.EndpointSliceConditions;
import io.gravitee.kubernetes.client.model.v1.EndpointSliceEndpoint;
import io.gravitee.kubernetes.client.model.v1.EndpointSlicePort;
import io.gravitee.kubernetes.client.model.v1.Event;
import io.gravitee.kubernetes.client.model.v1.KubernetesEventType;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.CustomLog;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KubernetesEventHandler {

  private static final long EMPTY_ENDPOINTS_HOLD_MS = 500L;

  private final Api api;
  private final EndpointManager endpointManager;
  private final EndpointGroup group;
  private final KubernetesServiceDiscoveryServiceConfiguration configuration;
  private final long pendingRequestsTimeout;
  private final Map<String, Set<String>> discoveredEndpoints;
  private final Map<String, Set<String>> sliceEndpoints = new HashMap<>();
  private Set<String> lastNonEmptyReady = new HashSet<>();
  private final AtomicLong emptyHoldSeq = new AtomicLong();
  private Disposable emptyHoldDisposable;

  public KubernetesEventHandler(
    Api api,
    EndpointManager endpointManager,
    EndpointGroup group,
    KubernetesServiceDiscoveryServiceConfiguration configuration,
    long pendingRequestsTimeout,
    Map<String, Set<String>> discoveredEndpoints
  ) {
    this.api = api;
    this.endpointManager = endpointManager;
    this.group = group;
    this.configuration = configuration;
    this.pendingRequestsTimeout = pendingRequestsTimeout;
    this.discoveredEndpoints = discoveredEndpoints;
  }

  public void handleSnapshot(List<EndpointSlice> endpointSlices) {
    log.debug(
      "Create endpoints from snapshot {} for api '{}'",
      endpointSlices,
      api.getName()
    );
    sliceEndpoints.clear();

    Set<String> notReady = new HashSet<>();
    endpointSlices.forEach(slice -> {
      String key = sliceKey(slice);
      if (key == null) {
        return;
      }
      sliceEndpoints.put(key, upsertFromEndpointSlice(slice));
      notReady.addAll(notReadyEndpointNames(slice));
    });

    Set<String> next = aggregateEndpoints();
    if (next.isEmpty() && notReady.isEmpty()) {
      // Avoid wiping all endpoints during transient empty snapshots with no signal.
      return;
    }
    applyDiscovered(next);
  }

  public void handle(Event<EndpointSlice> event) {
    log.debug("Handle event {} for api '{}'", event, api.getName());

    if (event == null || event.getObject() == null) {
      return;
    }
    if (KubernetesEventType.BOOKMARK.name().equals(event.getType())) {
      return;
    }

    if (KubernetesEventType.DELETED.name().equals(event.getType())) {
      String key = sliceKey(event.getObject());
      if (key == null) {
        return;
      }
      sliceEndpoints.remove(key);
      Set<String> next = aggregateEndpoints();
      applyDiscovered(next);
      return;
    }

    if (KubernetesEventType.ADDED.name().equals(event.getType())) {
      String key = sliceKey(event.getObject());
      if (key == null) {
        return;
      }
      Set<String> notReady = notReadyEndpointNames(event.getObject());
      sliceEndpoints.put(key, upsertFromEndpointSlice(event.getObject()));
      Set<String> next = aggregateEndpoints();
      if (next.isEmpty() && notReady.isEmpty()) {
        // Avoid clearing endpoints when the add has no signal.
        return;
      }
      applyDiscovered(next);
      return;
    }

    if (KubernetesEventType.MODIFIED.name().equals(event.getType())) {
      String key = sliceKey(event.getObject());
      if (key == null) {
        return;
      }
      Set<String> notReady = notReadyEndpointNames(event.getObject());
      sliceEndpoints.put(key, upsertFromEndpointSlice(event.getObject()));
      Set<String> next = aggregateEndpoints();
      if (next.isEmpty() && notReady.isEmpty()) {
        // Avoid clearing endpoints when the update has no signal.
        return;
      }
      applyDiscovered(next);
    }
  }

  private Set<String> upsertFromEndpointSlice(EndpointSlice slice) {
    Set<String> names = new HashSet<>();
    forEachEndpoint(slice, false, (address, port) -> {
      var endpoint = EndpointFactory.build(group, address, port, configuration);
      endpointManager.addOrUpdateEndpoint(group.getName(), endpoint);
      names.add(EndpointFactory.endpointName(address, port));
    });
    return names;
  }

  private Set<String> endpointNames(EndpointSlice slice) {
    Set<String> names = new HashSet<>();
    forEachEndpoint(slice, false, (address, port) ->
      names.add(EndpointFactory.endpointName(address, port))
    );
    return names;
  }

  private Set<String> notReadyEndpointNames(EndpointSlice slice) {
    Set<String> names = new HashSet<>();
    forEachEndpoint(slice, true, (address, port) ->
      names.add(EndpointFactory.endpointName(address, port))
    );
    return names;
  }

  private void forEachEndpoint(
    EndpointSlice slice,
    boolean notReady,
    EndpointConsumer consumer
  ) {
    if (slice.getEndpoints() == null || slice.getPorts() == null) {
      return;
    }

    Integer configuredPort = configuration.getPort();
    for (EndpointSliceEndpoint endpoint : slice.getEndpoints()) {
      if (!matchesReadiness(endpoint, notReady)) {
        continue;
      }
      List<String> addresses = endpoint.getAddresses();
      if (addresses == null || addresses.isEmpty()) {
        continue;
      }
      if (configuredPort != null) {
        if (!containsPort(slice.getPorts(), configuredPort)) {
          continue;
        }
        for (String address : addresses) {
          consumer.accept(toEndpointAddress(address), configuredPort);
        }
        continue;
      }
      for (EndpointSlicePort port : slice.getPorts()) {
        Integer resolvedPort = port.getPort();
        if (resolvedPort == null || resolvedPort <= 0) {
          continue;
        }
        for (String address : addresses) {
          consumer.accept(toEndpointAddress(address), resolvedPort);
        }
      }
    }
  }

  private boolean containsPort(List<EndpointSlicePort> ports, int desiredPort) {
    return ports
      .stream()
      .anyMatch(
        port -> port.getPort() != null && port.getPort() == desiredPort
      );
  }

  private boolean matchesReadiness(
    EndpointSliceEndpoint endpoint,
    boolean notReady
  ) {
    EndpointSliceConditions conditions = endpoint.getConditions();
    if (conditions != null) {
      if (Boolean.TRUE.equals(conditions.getTerminating())) {
        return notReady;
      }
      if (Boolean.FALSE.equals(conditions.getServing())) {
        return notReady;
      }
    }
    Boolean ready = conditions == null ? null : conditions.getReady();
    if (ready == null) {
      return !notReady;
    }
    return notReady != Boolean.TRUE.equals(ready);
  }

  private String sliceKey(EndpointSlice slice) {
    if (slice.getMetadata() == null) {
      return null;
    }
    String uid = slice.getMetadata().getUid();
    if (uid != null && !uid.isBlank()) {
      return uid;
    }
    String name = slice.getMetadata().getName();
    return name == null || name.isBlank() ? null : name;
  }

  private EndpointAddress toEndpointAddress(String address) {
    EndpointAddress endpointAddress = new EndpointAddress();
    endpointAddress.setIp(address);
    return endpointAddress;
  }

  private Set<String> aggregateEndpoints() {
    Set<String> next = new HashSet<>();
    sliceEndpoints.values().forEach(next::addAll);
    return next;
  }

  private Set<String> currentDiscovered() {
    return new HashSet<>(
      discoveredEndpoints.getOrDefault(group.getName(), new HashSet<>())
    );
  }

  private void applyDiscovered(Set<String> next) {
    Set<String> previous = currentDiscovered();
    if (!next.isEmpty()) {
      lastNonEmptyReady = new HashSet<>(next);
      if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
        emptyHoldDisposable.dispose();
      }
      updateDiscovered(next);
      return;
    }
    if (!lastNonEmptyReady.isEmpty()) {
      if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
        emptyHoldDisposable.dispose();
      }
      updateDiscovered(lastNonEmptyReady);
      return;
    }
    if (next.isEmpty() && !previous.isEmpty()) {
      long seq = emptyHoldSeq.incrementAndGet();
      if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
        emptyHoldDisposable.dispose();
      }
      emptyHoldDisposable = Completable.timer(
        EMPTY_ENDPOINTS_HOLD_MS,
        TimeUnit.MILLISECONDS,
        Schedulers.io()
      )
        .doOnComplete(() -> {
          if (emptyHoldSeq.get() == seq) {
            updateDiscovered(next);
          }
        })
        .subscribe();
      return;
    }
    if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
      emptyHoldDisposable.dispose();
    }
    updateDiscovered(next);
  }

  private void updateDiscovered(Set<String> next) {
    Set<String> previous = currentDiscovered();
    Set<String> removed = new HashSet<>(previous);
    removed.removeAll(next);

    removed.forEach(endpointManager::disable);
    Completable.defer(() -> {
      removed.forEach(endpointManager::removeEndpoint);
      return Completable.complete();
    })
      .onErrorComplete()
      .delaySubscription(
        pendingRequestsTimeout,
        TimeUnit.MILLISECONDS,
        Schedulers.io()
      )
      .subscribe();

    discoveredEndpoints.put(group.getName(), next);
  }

  @FunctionalInterface
  private interface EndpointConsumer {
    void accept(EndpointAddress address, int port);
  }
}
