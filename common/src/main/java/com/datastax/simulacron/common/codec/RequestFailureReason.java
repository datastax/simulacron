package com.datastax.simulacron.common.codec;

public enum RequestFailureReason {

  /**
   * The reason for the failure was none of the below reasons or was not recorded by the data node.
   */
  UNKNOWN(0x0000),

  /**
   * The data node read too many tombstones when attempting to execute a read query (see
   * tombstone_failure_threshold).
   */
  READ_TOO_MANY_TOMBSTONES(0x0001);

  private static final RequestFailureReason[] VALUES = values();

  /** The code to be serialized as an unsigned 16 bit integer */
  private final int code;

  RequestFailureReason(final int code) {
    this.code = code;
  }

  public static RequestFailureReason fromCode(final int code) {
    for (RequestFailureReason reasonCode : VALUES) {
      if (reasonCode.code == code) return reasonCode;
    }
    throw new IllegalArgumentException("Unknown request failure reason error code: " + code);
  }

  public int getCode() {
    return code;
  }
}
