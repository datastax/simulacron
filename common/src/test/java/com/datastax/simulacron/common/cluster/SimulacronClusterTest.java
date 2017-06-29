package com.datastax.simulacron.common.cluster;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SimulacronClusterTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testShouldThrowExceptionWhenUsingBuilderMethod() {
    thrown.expect(UnsupportedOperationException.class);
    SimulacronCluster.builder();
  }
}
