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

import io.gravitee.apim.plugin.apiservice.servicediscovery.kubernetes.KubernetesServiceDiscoveryServiceConfiguration;
import io.gravitee.kubernetes.client.model.v1.EndpointAddress;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.jspecify.annotations.NonNull;

public class HttpProxyEndpointConfigurationFactory
  implements EndpointConfigurationFactory {

  public static final String ENDPOINT_TYPE = "http-proxy";
  private static final String DEFAULT_SCHEME = "http";

  @Override
  public String buildConfiguration(
    EndpointAddress address,
    int port,
    KubernetesServiceDiscoveryServiceConfiguration configuration
  ) {
    var node = new JsonObject(
      Map.of("target", buildTargetUrl(address, port, configuration))
    );
    return node.toString();
  }

  private String buildTargetUrl(
    EndpointAddress address,
    int port,
    KubernetesServiceDiscoveryServiceConfiguration configuration
  ) {
    var scheme = buildScheme(configuration);
    var path = buildPath(configuration);
    var portSuffix = port > 0 ? ":" + port : "";
    return scheme + "://" + address.getIp() + portSuffix + path;
  }

  private static @NonNull String buildScheme(
    KubernetesServiceDiscoveryServiceConfiguration configuration
  ) {
    String scheme = configuration.getScheme();
    return scheme == null || scheme.isBlank() ? DEFAULT_SCHEME : scheme;
  }

  private String buildPath(
    KubernetesServiceDiscoveryServiceConfiguration configuration
  ) {
    String path = configuration.getPath();
    if (path != null && !path.isBlank()) {
      return removeTrailingSlash(path.startsWith("/") ? path : "/" + path);
    }
    return "";
  }

  private String removeTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
