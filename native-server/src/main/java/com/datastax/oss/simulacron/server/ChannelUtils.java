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
package com.datastax.oss.simulacron.server;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelGroupFutureListener;
import java.util.concurrent.CompletableFuture;

public class ChannelUtils {

  /**
   * Convenience method to convert a {@link ChannelFuture} into a {@link CompletableFuture}
   *
   * @param future future to convert.
   * @return converted future.
   */
  public static CompletableFuture<Void> completable(ChannelFuture future) {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    future.addListener(
        (ChannelFutureListener)
            future1 -> {
              if (future1.isSuccess()) {
                cf.complete(null);
              } else {
                cf.completeExceptionally(future1.cause());
              }
            });
    return cf;
  }

  public static CompletableFuture<Void> completable(ChannelGroupFuture future) {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    future.addListener(
        (ChannelGroupFutureListener)
            future1 -> {
              if (future1.isSuccess()) {
                cf.complete(null);
              } else {
                cf.completeExceptionally(future1.cause());
              }
            });
    return cf;
  }
}
