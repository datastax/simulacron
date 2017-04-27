package com.datastax.simulacron.http.server;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A simple pojo for returning informational messages in a response where no other payload is
 * applicable otherwise.
 */
public class Message {
  @JsonProperty("message")
  protected String message;

  @JsonProperty("status_code")
  protected int statusCode;

  public Message() {
    // Unused, for Jackson only
  }

  public Message(String message, int statusCode) {
    this.message = message;
    this.statusCode = statusCode;
  }

  @Override
  public String toString() {
    return "Message{" + "message='" + message + '\'' + ", statusCode=" + statusCode + '}';
  }

  /** @return The message associated with this message. */
  public String getMessage() {
    return message;
  }

  /** @return The HTTP status code associated with this message. */
  public int getStatusCode() {
    return statusCode;
  }
}
