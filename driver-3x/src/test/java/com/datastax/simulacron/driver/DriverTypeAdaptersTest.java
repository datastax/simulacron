package com.datastax.simulacron.driver;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.WriteType;
import org.junit.Test;

import static com.datastax.simulacron.common.codec.ConsistencyLevel.*;
import static com.datastax.simulacron.common.codec.WriteType.*;
import static com.datastax.simulacron.driver.DriverTypeAdapters.adapt;
import static com.datastax.simulacron.driver.DriverTypeAdapters.extract;
import static org.assertj.core.api.Assertions.assertThat;

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
