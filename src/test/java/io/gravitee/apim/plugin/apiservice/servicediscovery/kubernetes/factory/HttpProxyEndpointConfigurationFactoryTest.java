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
package io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.factory;

import static org.assertj.core.api.Assertions.assertThat;

import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.KubernetesServiceDiscoveryServiceConfiguration;
import io.gravitee.kubernetes.client.model.v1.EndpointAddress;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class HttpProxyEndpointConfigurationFactoryTest {

  private HttpProxyEndpointConfigurationFactory factory;

  @BeforeEach
  void setUp() {
    factory = new HttpProxyEndpointConfigurationFactory();
  }

  @Test
  void should_build_configuration_with_default_values() {
    EndpointAddress address = new EndpointAddress();
    address.setIp("10.0.0.1");
    int port = 8080;
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();

    String result = factory.buildConfiguration(address, port, configuration);

    JsonObject json = new JsonObject(result);
    assertThat(json.getString("target")).isEqualTo("http://10.0.0.1:8080");
  }

  @Test
  void should_build_configuration_with_custom_scheme() {
    EndpointAddress address = new EndpointAddress();
    address.setIp("10.0.0.1");
    int port = 8443;
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();
    configuration.setScheme("https");

    String result = factory.buildConfiguration(address, port, configuration);

    JsonObject json = new JsonObject(result);
    assertThat(json.getString("target")).isEqualTo("https://10.0.0.1:8443");
  }

  @ParameterizedTest
  @CsvSource({ "/my-path", "/my-path", "my-path" })
  void should_build_configuration_with_custom_path(String inputPath) {
    EndpointAddress address = new EndpointAddress();
    address.setIp("10.0.0.1");
    int port = 8080;
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();
    configuration.setPath(inputPath);

    String result = factory.buildConfiguration(address, port, configuration);

    JsonObject json = new JsonObject(result);
    assertThat(json.getString("target")).isEqualTo(
      "http://10.0.0.1:8080/my-path"
    );
  }

  @Test
  void should_build_configuration_without_port_suffix_when_port_is_zero() {
    EndpointAddress address = new EndpointAddress();
    address.setIp("10.0.0.1");
    int port = 0;
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();

    String result = factory.buildConfiguration(address, port, configuration);

    JsonObject json = new JsonObject(result);
    assertThat(json.getString("target")).isEqualTo("http://10.0.0.1");
  }

  @Test
  void should_build_configuration_without_port_suffix_when_port_is_negative() {
    EndpointAddress address = new EndpointAddress();
    address.setIp("10.0.0.1");
    int port = -1;
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();

    String result = factory.buildConfiguration(address, port, configuration);

    JsonObject json = new JsonObject(result);
    assertThat(json.getString("target")).isEqualTo("http://10.0.0.1");
  }

  @Test
  void should_build_configuration_with_all_custom_values() {
    EndpointAddress address = new EndpointAddress();
    address.setIp("10.0.0.2");
    int port = 9000;
    KubernetesServiceDiscoveryServiceConfiguration configuration =
      new KubernetesServiceDiscoveryServiceConfiguration();
    configuration.setScheme("https");
    configuration.setPath("/api/v1/");

    String result = factory.buildConfiguration(address, port, configuration);

    JsonObject json = new JsonObject(result);
    assertThat(json.getString("target")).isEqualTo(
      "https://10.0.0.2:9000/api/v1"
    );
  }
}
