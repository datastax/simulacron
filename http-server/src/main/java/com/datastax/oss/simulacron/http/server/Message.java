/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.simulacron.http.server;

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
