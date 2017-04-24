package com.datastax.simulacron.common.codec;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsistencyLevelTest {

  @Test
  public void testFromString() {
    ConsistencyLevel CL = ConsistencyLevel.fromString("any");
    ConsistencyLevel CL2 = ConsistencyLevel.fromString("ANY");
    assertThat(CL).isEqualTo(ConsistencyLevel.ANY);
    assertThat(CL2).isEqualTo(ConsistencyLevel.ANY);

    CL = ConsistencyLevel.fromString("local_one");
    CL2 = ConsistencyLevel.fromString("LOcaL_oNe");
    assertThat(CL2).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  }

  @Test
  public void testAll() {
    for (ConsistencyLevel level : ConsistencyLevel.values()) {
      assertThat(ConsistencyLevel.fromString(level.name())).isEqualTo(level);
      assertThat(ConsistencyLevel.fromString(level.name().toLowerCase())).isEqualTo(level);
    }
  }

  @Test
  public void testFromCode() {
    ConsistencyLevel cl = ConsistencyLevel.fromCode(1);
    ConsistencyLevel cl2 = ConsistencyLevel.fromCode(10);
    assertThat(cl).isEqualTo(ConsistencyLevel.ONE);
    assertThat(cl2).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  }
}
