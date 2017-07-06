package com.datastax.simulacron.server;

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
