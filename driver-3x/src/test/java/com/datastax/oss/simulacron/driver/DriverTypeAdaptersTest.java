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
package com.datastax.oss.simulacron.driver;

import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.QUORUM;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.SERIAL;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.THREE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.TWO;
import static com.datastax.oss.simulacron.common.codec.WriteType.BATCH;
import static com.datastax.oss.simulacron.common.codec.WriteType.BATCH_LOG;
import static com.datastax.oss.simulacron.common.codec.WriteType.CAS;
import static com.datastax.oss.simulacron.common.codec.WriteType.COUNTER;
import static com.datastax.oss.simulacron.common.codec.WriteType.SIMPLE;
import static com.datastax.oss.simulacron.common.codec.WriteType.UNLOGGED_BATCH;
import static com.datastax.oss.simulacron.driver.DriverTypeAdapters.adapt;
import static com.datastax.oss.simulacron.driver.DriverTypeAdapters.extract;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.WriteType;
import org.junit.Test;

public class DriverTypeAdaptersTest {

  @Test
  public void testShouldAdaptConsistencyLevels() {
    assertThat(adapt(ConsistencyLevel.ANY)).isEqualTo(ANY);
    assertThat(adapt(ConsistencyLevel.ONE)).isSameAs(ONE);
    assertThat(adapt(ConsistencyLevel.TWO)).isSameAs(TWO);
    assertThat(adapt(ConsistencyLevel.THREE)).isSameAs(THREE);
    assertThat(adapt(ConsistencyLevel.QUORUM)).isSameAs(QUORUM);
    assertThat(adapt(ConsistencyLevel.ALL)).isSameAs(ALL);
    assertThat(adapt(ConsistencyLevel.LOCAL_QUORUM)).isSameAs(LOCAL_QUORUM);
    assertThat(adapt(ConsistencyLevel.EACH_QUORUM)).isSameAs(EACH_QUORUM);
    assertThat(adapt(ConsistencyLevel.SERIAL)).isSameAs(SERIAL);
    assertThat(adapt(ConsistencyLevel.LOCAL_SERIAL)).isSameAs(LOCAL_SERIAL);
    assertThat(adapt(ConsistencyLevel.LOCAL_ONE)).isSameAs(LOCAL_ONE);
  }

  @Test
  public void testShouldExtractConsistencyLevels() {
    assertThat(extract(ANY)).isEqualTo(ConsistencyLevel.ANY);
    assertThat(extract(ONE)).isSameAs(ConsistencyLevel.ONE);
    assertThat(extract(TWO)).isSameAs(ConsistencyLevel.TWO);
    assertThat(extract(THREE)).isSameAs(ConsistencyLevel.THREE);
    assertThat(extract(QUORUM)).isSameAs(ConsistencyLevel.QUORUM);
    assertThat(extract(ALL)).isSameAs(ConsistencyLevel.ALL);
    assertThat(extract(LOCAL_QUORUM)).isSameAs(ConsistencyLevel.LOCAL_QUORUM);
    assertThat(extract(EACH_QUORUM)).isSameAs(ConsistencyLevel.EACH_QUORUM);
    assertThat(extract(SERIAL)).isSameAs(ConsistencyLevel.SERIAL);
    assertThat(extract(LOCAL_SERIAL)).isSameAs(ConsistencyLevel.LOCAL_SERIAL);
    assertThat(extract(LOCAL_ONE)).isSameAs(ConsistencyLevel.LOCAL_ONE);
  }

  @Test
  public void testShouldAdaptExtractConsistencyLevels() {
    for (ConsistencyLevel c : ConsistencyLevel.values()) {
      assertThat(extract(adapt(c))).isSameAs(c);
    }
  }

  @Test
  public void testShouldAdaptWriteTypes() {
    assertThat(adapt(WriteType.SIMPLE)).isSameAs(SIMPLE);
    assertThat(adapt(WriteType.BATCH)).isSameAs(BATCH);
    assertThat(adapt(WriteType.UNLOGGED_BATCH)).isSameAs(UNLOGGED_BATCH);
    assertThat(adapt(WriteType.COUNTER)).isSameAs(COUNTER);
    assertThat(adapt(WriteType.BATCH_LOG)).isSameAs(BATCH_LOG);
    assertThat(adapt(WriteType.CAS)).isSameAs(CAS);
  }

  @Test
  public void testShouldExtractWriteTypes() {
    assertThat(extract(SIMPLE)).isSameAs(WriteType.SIMPLE);
    assertThat(extract(BATCH)).isSameAs(WriteType.BATCH);
    assertThat(extract(UNLOGGED_BATCH)).isSameAs(WriteType.UNLOGGED_BATCH);
    assertThat(extract(COUNTER)).isSameAs(WriteType.COUNTER);
    assertThat(extract(BATCH_LOG)).isSameAs(WriteType.BATCH_LOG);
    assertThat(extract(CAS)).isSameAs(WriteType.CAS);
  }

  @Test
  public void testShouldAdaptExtractWriteTypes() {
    for (WriteType w : WriteType.values()) {
      assertThat(extract(adapt(w))).isSameAs(w);
    }
  }
}
