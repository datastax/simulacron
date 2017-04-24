package com.datastax.simulacron.http.server;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/** POJO for Error messages that can be serialized/deserialized using jackson. */
public class ErrorMessage {
  @JsonProperty("message")
  private final String message;

  @JsonProperty("status_code")
  private final int statusCode;

  @JsonIgnore private transient Throwable exception;

  public ErrorMessage(String message, int statusCode) {
    this(message, statusCode, null);
  }

  public ErrorMessage(String message, int statusCode, Throwable exception) {
    this.message = message;
    this.statusCode = statusCode;
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

  /** @return The message associated with this error. */
  public String getMessage() {
    return message;
  }

  /** @return The HTTP status code associated with this error. */
  public int getStatusCode() {
    return statusCode;
  }

  /** @return The exception associated with this error (if provided). */
  public Throwable getException() {
    return exception;
  }
}
