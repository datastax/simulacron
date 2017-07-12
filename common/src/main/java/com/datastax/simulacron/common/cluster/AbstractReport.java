package com.datastax.simulacron.common.cluster;

import java.util.Optional;

/** Implements the id Portion of the Report interface. must be implemented at the class level */
public abstract class AbstractReport implements Report {

  private final Long id;

  AbstractReport(Long id) {
    this.id = id;
  }
  /** @return A unique id for this. */
  public Long getId() {
    return id;
  }

  public abstract Optional<AbstractReport> getParent();
}
