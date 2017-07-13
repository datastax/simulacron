package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public interface Identifiable extends Comparable<Identifiable> {
  /** @return A unique id for this. */
  @JsonInclude(NON_NULL)
  Long getId();

  @Override
  default int compareTo(Identifiable other) {
    // by default compare by id, this is not perfect if comparing cross-dc or comparing different types but in the
    // general case this should only be used for comparing for things in same group.
    return (int) (this.getId() - other.getId());
  }
}
