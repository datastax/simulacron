package com.datastax.simulacron.common.cluster;

public interface Identifiable extends Comparable<Identifiable> {
  /** @return A unique id for this. */
  Long getId();

  @Override
  default int compareTo(Identifiable other) {
    // by default compare by id, this is not perfect if comparing cross-dc or comparing different types but in the
    // general case this should only be used for comparing for things in same group.
    return (int) (this.getId() - other.getId());
  }
}
