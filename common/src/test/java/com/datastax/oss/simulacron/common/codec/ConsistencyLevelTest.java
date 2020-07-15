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
package com.datastax.oss.simulacron.common.codec;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

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
