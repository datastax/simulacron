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
package com.datastax.oss.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Message;

/** An action that involves sending a given {@link Message} as a response. */
public class MessageResponseAction implements Action {

  private final Message message;
  private final long delayInMs;

  /**
   * Constructs an action to send the input message immediately.
   *
   * @param message The message to send.
   */
  public MessageResponseAction(Message message) {
    this(message, 0);
  }

  /**
   * Constructs an action to send the input message at some time in the future.
   *
   * @param message The message to send.
   * @param delayInMs How much to delay sending the action.
   */
  public MessageResponseAction(Message message, long delayInMs) {
    this.message = message;
    this.delayInMs = delayInMs;
  }

  /** @return The message to send. */
  public Message getMessage() {
    return message;
  }

  @Override
  public Long delayInMs() {
    return delayInMs;
  }
}
