package com.datastax.simulacron.server;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

class CompletableFutures {

  static <T> T getUninterruptibly(CompletionStage<T> stage) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return stage.toCompletableFuture().get();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          Throwable throwable = e.getCause();
          if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
          } else if (throwable instanceof Error) {
            throw (Error) throwable;
          } else {
            // TODO: maybe wrap in custom exception?
            throw new RuntimeException(throwable);
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
