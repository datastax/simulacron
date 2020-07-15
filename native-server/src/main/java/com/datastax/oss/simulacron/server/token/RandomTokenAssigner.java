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
package com.datastax.oss.simulacron.server.token;

import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Random token generator */
public class RandomTokenAssigner extends TokenAssigner {

  /** Random used to generate tokens for virtual nodes. */
  private static final Random random = new Random();

  /** Number of tokens to assign per node */
  private final int numberOfTokens;

  /** Longs that have been previous used, ensures uniqueness of assigned tokens. */
  private final Set<Long> usedLongs = new TreeSet<>();

  public RandomTokenAssigner(int tokens) {
    this.numberOfTokens = tokens;
  }

  @Override
  public String getTokensInternal(NodeSpec nodeSpec) {
    Set<Long> generatedTokens = new TreeSet<>();

    while (generatedTokens.size() < numberOfTokens) {
      generatedTokens.addAll(
          random
              .longs(numberOfTokens - generatedTokens.size())
              .filter(l -> !generatedTokens.contains(l) && !usedLongs.contains(l))
              .mapToObj(Long::new)
              .collect(Collectors.toSet()));
    }

    usedLongs.addAll(generatedTokens);
    return generatedTokens.stream().map(Object::toString).collect(Collectors.joining(","));
  }
}
