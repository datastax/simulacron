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
package com.datastax.oss.simulacron.http.server;

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
