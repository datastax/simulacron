package com.datastax.simulacron.server;

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
