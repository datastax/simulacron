/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.simulacron.server;

/** A set of options use to configure how a Cluster/Node is registered with a {@link Server}. */
public class ServerOptions {

  private final Boolean activityLogging;

  ServerOptions(Boolean activityLogging) {
    this.activityLogging = activityLogging;
  }

  /**
   * @return Whether or not activity logging should be enabled or null to defer to global
   *     configuration.
   */
  public Boolean isActivityLoggingEnabled() {
    return activityLogging;
  }

  public static ServerOptions DEFAULT = new ServerOptions(null);

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Boolean activityLogging = null;

    /**
     * Whether or not to enable activity logging. By default falls back on global configuration
     * (which defaults to enabled).
     *
     * @param enabled enablement flag. Set to null to fallback.
     * @return This builder.
     */
    public Builder withActivityLoggingEnabled(Boolean enabled) {
      this.activityLogging = enabled;
      return this;
    }

    public ServerOptions build() {
      return new ServerOptions(activityLogging);
    }
  }
}
