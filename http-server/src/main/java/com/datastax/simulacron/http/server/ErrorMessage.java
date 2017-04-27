package com.datastax.simulacron.http.server;

import com.fasterxml.jackson.annotation.JsonIgnore;

/** POJO for Error messages that can be serialized/deserialized using jackson. */
public class ErrorMessage extends Message {
  @JsonIgnore private transient Throwable exception;

  public ErrorMessage() {
    // Unused, for Jackson only
  }

  public ErrorMessage(String message, int statusCode) {
    this(message, statusCode, null);
  }

  public ErrorMessage(String message, int statusCode, Throwable exception) {
    super(message, statusCode);
    this.exception = exception;
  }

  public ErrorMessage(Throwable exception, int statusCode) {
    this(exception.getMessage(), statusCode, exception);
  }

  @Override
  public String toString() {
    return "ErrorMessage{"
        + "message='"
        + message
        + '\''
        + ", statusCode="
        + statusCode
        + ", exception="
        + exception
        + '}';
  }

  /** @return The exception associated with this error (if provided). */
  public Throwable getException() {
    return exception;
  }
}
