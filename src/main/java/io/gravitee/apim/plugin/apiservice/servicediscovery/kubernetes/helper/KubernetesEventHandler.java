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

  private static final long DEFAULT_EMPTY_ENDPOINTS_HOLD_MS = 300L;
  private static final long DEFAULT_FALLBACK_TTL_MS = 30_000L;
  private static final long DEFAULT_DRAIN_TTL_MS = 1000L;
  private static final long DEFAULT_COALESCE_WINDOW_MS = 200L;

  private final Api api;
  private final EndpointManager endpointManager;
  private final EndpointGroup group;
  private final KubernetesServiceDiscoveryServiceConfiguration configuration;
  private final long pendingRequestsTimeout;
  private final long emptyEndpointsHoldMs;
  private final long fallbackTtlMs;
  private final long drainTtlMs;
  private final Map<String, Set<String>> discoveredEndpoints;
  private final Map<String, Set<String>> sliceReadyEndpoints = new HashMap<>();
  private final Map<String, Set<String>> sliceDrainingEndpoints =
    new HashMap<>();
  private final Map<String, EndpointSlice> lastSlices = new HashMap<>();
  private Set<String> lastNonEmptyReady = new HashSet<>();
  private long lastNonEmptyReadyAt = 0L;
  private long drainHoldUntilMs = 0L;
  private final AtomicLong emptyHoldSeq = new AtomicLong();
  private Disposable emptyHoldDisposable;
  private final Object eventLock = new Object();
  private final Map<String, Event<EndpointSlice>> pendingEvents =
    new HashMap<>();
  private final AtomicLong flushSeq = new AtomicLong();
  private Disposable flushDisposable;
  private final long coalesceWindowMs = DEFAULT_COALESCE_WINDOW_MS;

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
    this.emptyEndpointsHoldMs = resolveOrDefault(
      configuration.getEmptyHoldMs(),
      DEFAULT_EMPTY_ENDPOINTS_HOLD_MS
    );
    this.fallbackTtlMs = resolveOrDefault(
      configuration.getFallbackTtlMs(),
      DEFAULT_FALLBACK_TTL_MS
    );
    this.drainTtlMs = resolveOrDefault(
      configuration.getDrainTtlMs(),
      DEFAULT_DRAIN_TTL_MS
    );
  }

  public void handleSnapshot(List<EndpointSlice> endpointSlices) {
    log.debug(
      "Create endpoints from snapshot {} for api '{}'",
      endpointSlices,
      api.getName()
    );
    sliceReadyEndpoints.clear();
    sliceDrainingEndpoints.clear();
    lastSlices.clear();

    Set<String> notReady = new HashSet<>();
    endpointSlices.forEach(slice -> {
      String key = sliceKey(slice);
      if (key == null) {
        return;
      }
      lastSlices.put(key, slice);
      sliceReadyEndpoints.put(key, readyEndpointNames(slice));
      sliceDrainingEndpoints.put(key, drainingEndpointNames(slice));
      notReady.addAll(notReadyEndpointNames(slice));
    });

    Set<String> nextReady = aggregateReadyEndpoints();
    Set<String> nextDraining = aggregateDrainingEndpoints();
    if (nextReady.isEmpty() && nextDraining.isEmpty() && notReady.isEmpty()) {
      // Avoid wiping all endpoints during transient empty snapshots with no signal.
      return;
    }
    applyDiscovered(nextReady, nextDraining);
  }

  public void handle(Event<EndpointSlice> event) {
    if (coalesceWindowMs <= 0) {
      handleImmediate(event);
      return;
    }
    if (event == null || event.getObject() == null) {
      return;
    }
    if (KubernetesEventType.BOOKMARK.name().equals(event.getType())) {
      return;
    }
    String key = sliceKey(event.getObject());
    if (key == null) {
      return;
    }
    synchronized (eventLock) {
      pendingEvents.put(key, event);
      long seq = flushSeq.incrementAndGet();
      if (flushDisposable != null && !flushDisposable.isDisposed()) {
        flushDisposable.dispose();
      }
      flushDisposable = Completable.timer(
        coalesceWindowMs,
        TimeUnit.MILLISECONDS,
        Schedulers.io()
      )
        .doOnComplete(() -> flushPending(seq))
        .subscribe();
    }
  }

  private void flushPending(long seq) {
    Map<String, Event<EndpointSlice>> events;
    synchronized (eventLock) {
      if (flushSeq.get() != seq) {
        return;
      }
      events = new HashMap<>(pendingEvents);
      pendingEvents.clear();
      if (flushDisposable != null && !flushDisposable.isDisposed()) {
        flushDisposable.dispose();
      }
    }
    events.values().forEach(this::handleImmediate);
  }

  private void handleImmediate(Event<EndpointSlice> event) {
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
      sliceReadyEndpoints.remove(key);
      sliceDrainingEndpoints.remove(key);
      lastSlices.remove(key);
      Set<String> nextReady = aggregateReadyEndpoints();
      Set<String> nextDraining = aggregateDrainingEndpoints();
      applyDiscovered(nextReady, nextDraining);
      return;
    }

    if (
      KubernetesEventType.ADDED.name().equals(event.getType()) ||
      KubernetesEventType.MODIFIED.name().equals(event.getType())
    ) {
      String key = sliceKey(event.getObject());
      if (key == null) {
        return;
      }
      EndpointSlice previous = lastSlices.get(key);
      if (
        previous != null && !endpointSliceChanged(previous, event.getObject())
      ) {
        return;
      }
      lastSlices.put(key, event.getObject());
      Set<String> notReady = notReadyEndpointNames(event.getObject());
      sliceReadyEndpoints.put(key, readyEndpointNames(event.getObject()));
      sliceDrainingEndpoints.put(key, drainingEndpointNames(event.getObject()));
      Set<String> nextReady = aggregateReadyEndpoints();
      Set<String> nextDraining = aggregateDrainingEndpoints();
      if (nextReady.isEmpty() && nextDraining.isEmpty() && notReady.isEmpty()) {
        // Avoid clearing endpoints when the update has no signal.
        return;
      }
      applyDiscovered(nextReady, nextDraining);
    }
  }

  private boolean endpointSliceChanged(EndpointSlice a, EndpointSlice b) {
    if (a == null || b == null) {
      return true;
    }
    if (a.getPorts() == null || b.getPorts() == null) {
      return a.getPorts() != b.getPorts();
    }
    if (a.getPorts().size() != b.getPorts().size()) {
      return true;
    }
    for (int i = 0; i < a.getPorts().size(); i++) {
      EndpointSlicePort ap = a.getPorts().get(i);
      EndpointSlicePort bp = b.getPorts().get(i);
      if (!nullSafeEquals(ap.getName(), bp.getName())) {
        return true;
      }
      if (!nullSafeEquals(ap.getPort(), bp.getPort())) {
        return true;
      }
      if (!nullSafeEquals(ap.getProtocol(), bp.getProtocol())) {
        return true;
      }
    }

    if (a.getEndpoints() == null || b.getEndpoints() == null) {
      return a.getEndpoints() != b.getEndpoints();
    }
    if (a.getEndpoints().size() != b.getEndpoints().size()) {
      return true;
    }
    for (int i = 0; i < a.getEndpoints().size(); i++) {
      EndpointSliceEndpoint ea = a.getEndpoints().get(i);
      EndpointSliceEndpoint eb = b.getEndpoints().get(i);
      if (!nullSafeEquals(ea.getAddresses(), eb.getAddresses())) {
        return true;
      }
      EndpointSliceConditions ca = ea.getConditions();
      EndpointSliceConditions cb = eb.getConditions();
      if (
        !nullSafeEquals(
          ca == null ? null : ca.getReady(),
          cb == null ? null : cb.getReady()
        )
      ) {
        return true;
      }
      if (
        !nullSafeEquals(
          ca == null ? null : ca.getServing(),
          cb == null ? null : cb.getServing()
        )
      ) {
        return true;
      }
      if (
        !nullSafeEquals(
          ca == null ? null : ca.getTerminating(),
          cb == null ? null : cb.getTerminating()
        )
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean nullSafeEquals(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  private Set<String> readyEndpointNames(EndpointSlice slice) {
    Set<String> names = new HashSet<>();
    forEachEndpoint(slice, EndpointState.READY, (address, port) ->
      names.add(EndpointFactory.endpointName(address, port))
    );
    return names;
  }

  private Set<String> drainingEndpointNames(EndpointSlice slice) {
    Set<String> names = new HashSet<>();
    forEachEndpoint(slice, EndpointState.DRAINING, (address, port) ->
      names.add(EndpointFactory.endpointName(address, port))
    );
    return names;
  }

  private Set<String> notReadyEndpointNames(EndpointSlice slice) {
    Set<String> names = new HashSet<>();
    forEachEndpoint(slice, EndpointState.NOT_READY, (address, port) ->
      names.add(EndpointFactory.endpointName(address, port))
    );
    return names;
  }

  private void forEachEndpoint(
    EndpointSlice slice,
    EndpointState desiredState,
    EndpointConsumer consumer
  ) {
    if (slice.getEndpoints() == null || slice.getPorts() == null) {
      return;
    }

    Integer configuredPort = configuration.getPort();
    for (EndpointSliceEndpoint endpoint : slice.getEndpoints()) {
      if (classify(endpoint) != desiredState) {
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

  private EndpointState classify(EndpointSliceEndpoint endpoint) {
    EndpointSliceConditions conditions = endpoint.getConditions();
    boolean serving =
      conditions == null ||
      conditions.getServing() == null ||
      Boolean.TRUE.equals(conditions.getServing());
    if (!serving) {
      return EndpointState.NOT_READY;
    }
    boolean terminating =
      conditions != null && Boolean.TRUE.equals(conditions.getTerminating());
    if (terminating) {
      return EndpointState.DRAINING;
    }
    Boolean ready = conditions == null ? null : conditions.getReady();
    boolean isReady = ready == null || Boolean.TRUE.equals(ready);
    return isReady ? EndpointState.READY : EndpointState.NOT_READY;
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

  private void ensureEndpointsPresent(Set<String> names) {
    for (String name : names) {
      EndpointDescriptor descriptor = parseEndpointName(name);
      if (descriptor == null) {
        continue;
      }
      var endpoint = EndpointFactory.build(
        group,
        toEndpointAddress(descriptor.address),
        descriptor.port,
        configuration
      );
      endpointManager.addOrUpdateEndpoint(group.getName(), endpoint);
    }
  }

  private EndpointDescriptor parseEndpointName(String name) {
    if (name == null || !name.startsWith("kubernetes#")) {
      return null;
    }
    int lastSep = name.lastIndexOf('#');
    if (lastSep <= "kubernetes#".length() || lastSep == name.length() - 1) {
      return null;
    }
    String encodedIp = name.substring("kubernetes#".length(), lastSep);
    String portRaw = name.substring(lastSep + 1);
    int port;
    try {
      port = Integer.parseInt(portRaw);
    } catch (NumberFormatException ex) {
      return null;
    }
    String ip = encodedIp.replace("#", ":");
    return new EndpointDescriptor(ip, port);
  }

  private Set<String> aggregateReadyEndpoints() {
    Set<String> next = new HashSet<>();
    sliceReadyEndpoints.values().forEach(next::addAll);
    return next;
  }

  private Set<String> aggregateDrainingEndpoints() {
    Set<String> next = new HashSet<>();
    sliceDrainingEndpoints.values().forEach(next::addAll);
    return next;
  }

  private Set<String> currentDiscovered() {
    return new HashSet<>(
      discoveredEndpoints.getOrDefault(group.getName(), new HashSet<>())
    );
  }

  private void applyDiscovered(
    Set<String> nextReady,
    Set<String> nextDraining
  ) {
    Set<String> previous = currentDiscovered();
    if (!nextReady.isEmpty()) {
      lastNonEmptyReady = new HashSet<>(nextReady);
      lastNonEmptyReadyAt = System.currentTimeMillis();
      drainHoldUntilMs = 0L;
      if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
        emptyHoldDisposable.dispose();
      }
      updateDiscovered(nextReady);
      return;
    }

    if (!nextDraining.isEmpty() && drainTtlMs > 0) {
      long now = System.currentTimeMillis();
      if (drainHoldUntilMs == 0L || now > drainHoldUntilMs) {
        drainHoldUntilMs = now + drainTtlMs;
      }
      if (now <= drainHoldUntilMs) {
        if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
          emptyHoldDisposable.dispose();
        }
        updateDiscovered(nextDraining);
        return;
      }
    }

    if (!lastNonEmptyReady.isEmpty()) {
      if (fallbackTtlMs > 0 && lastNonEmptyReadyAt > 0) {
        long ageMs = System.currentTimeMillis() - lastNonEmptyReadyAt;
        if (ageMs > fallbackTtlMs) {
          lastNonEmptyReady = new HashSet<>();
        }
      }
    }
    if (!lastNonEmptyReady.isEmpty()) {
      if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
        emptyHoldDisposable.dispose();
      }
      updateDiscovered(lastNonEmptyReady);
      return;
    }
    if (nextReady.isEmpty() && nextDraining.isEmpty() && !previous.isEmpty()) {
      long seq = emptyHoldSeq.incrementAndGet();
      if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
        emptyHoldDisposable.dispose();
      }
      emptyHoldDisposable = Completable.timer(
        emptyEndpointsHoldMs,
        TimeUnit.MILLISECONDS,
        Schedulers.io()
      )
        .doOnComplete(() -> {
          if (emptyHoldSeq.get() == seq) {
            updateDiscovered(new HashSet<>());
          }
        })
        .subscribe();
      return;
    }
    if (emptyHoldDisposable != null && !emptyHoldDisposable.isDisposed()) {
      emptyHoldDisposable.dispose();
    }
    updateDiscovered(new HashSet<>());
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

  private long resolveOrDefault(Long value, long fallback) {
    if (value == null) {
      return fallback;
    }
    return value;
  }

  private enum EndpointState {
    READY,
    DRAINING,
    NOT_READY,
  }

  private static final class EndpointDescriptor {

    private final String address;
    private final int port;

    private EndpointDescriptor(String address, int port) {
      this.address = address;
      this.port = port;
    }
  }

  @FunctionalInterface
  private interface EndpointConsumer {
    void accept(EndpointAddress address, int port);
  }
}
