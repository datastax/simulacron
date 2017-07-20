package com.datastax.simulacron.server.token;

import com.datastax.simulacron.common.cluster.NodeSpec;
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
