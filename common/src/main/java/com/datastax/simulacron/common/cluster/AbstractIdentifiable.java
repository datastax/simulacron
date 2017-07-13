package com.datastax.simulacron.common.cluster;

/**
 * Implements the id Portion of the Identifiable interface. must be implemented at the class level
 */
public abstract class AbstractIdentifiable implements Identifiable {

  private final Long id;

  AbstractIdentifiable(Long id) {
    this.id = id;
  }
  /** @return A unique id for this. */
  public Long getId() {
    return id;
  }
}
