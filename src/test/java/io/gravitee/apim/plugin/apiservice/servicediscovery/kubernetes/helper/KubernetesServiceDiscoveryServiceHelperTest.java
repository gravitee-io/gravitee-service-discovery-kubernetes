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
import io.gravitee.kubernetes.client.config.KubernetesConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KubernetesServiceDiscoveryServiceHelperTest {

  private final KubernetesConfig kubernetesConfig =
    KubernetesConfig.getInstance();
  private String previousNamespace;
  private static final String CUSTOM_NAMESPACE = "custom";
  private static final String FALLBACK_NAMESPACE = "fallback";
  private static final String DEFAULT_NAMESPACE = "default";

  @AfterEach
  void tearDown() {
    kubernetesConfig.setCurrentNamespace(previousNamespace);
  }

  @Test
  void should_prefer_configured_namespace() {
    previousNamespace = kubernetesConfig.getCurrentNamespace();
    kubernetesConfig.setCurrentNamespace(FALLBACK_NAMESPACE);

    KubernetesServiceDiscoveryServiceConfiguration config =
      new KubernetesServiceDiscoveryServiceConfiguration();
    config.setNamespace(CUSTOM_NAMESPACE);

    assertThat(
      KubernetesServiceDiscoveryServiceHelper.resolveNamespace(config)
    ).isEqualTo(CUSTOM_NAMESPACE);
  }

  @Test
  void should_default_namespace_when_unavailable() {
    previousNamespace = kubernetesConfig.getCurrentNamespace();
    kubernetesConfig.setCurrentNamespace("");

    KubernetesServiceDiscoveryServiceConfiguration config =
      new KubernetesServiceDiscoveryServiceConfiguration();

    assertThat(
      KubernetesServiceDiscoveryServiceHelper.resolveNamespace(config)
    ).isEqualTo(DEFAULT_NAMESPACE);
  }
}
