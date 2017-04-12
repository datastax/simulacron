package com.datastax.simulacron.common.stubbing;

/**
 * Defines some behavior to take place, optionally at some point in the future (defined by {@link
 * #delayInMs}).
 */
public interface Action {
  /** @return How far into the future to schedule this action. */
  Long delayInMs();
}
