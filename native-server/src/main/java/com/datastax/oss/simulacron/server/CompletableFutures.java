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
package com.datastax.oss.simulacron.server;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

  static <T> T getUninterruptibly(CompletionStage<T> stage, long time, TimeUnit timeUnit) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return stage.toCompletableFuture().get(time, timeUnit);
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException | TimeoutException e) {
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
