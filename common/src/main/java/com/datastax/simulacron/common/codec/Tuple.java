package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType.RawTuple;

import java.util.Arrays;
import java.util.List;

public class Tuple {

  private final RawTuple tupleType;
  private final List<Object> values;

  public Tuple(RawTuple tupleType, Object... values) {
    this(tupleType, Arrays.asList(values));
  }

  public Tuple(RawTuple tupleType, List<Object> values) {
    this.tupleType = tupleType;
    this.values = values;
  }

  public RawTuple getTupleType() {
    return tupleType;
  }

  public List<Object> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Tuple tuple = (Tuple) o;

    if (!tupleType.equals(tuple.tupleType)) return false;
    return values.equals(tuple.values);
  }

  @Override
  public int hashCode() {
    int result = tupleType.hashCode();
    result = 31 * result + values.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Tuple{" + "tupleType=" + tupleType + ", values=" + values + '}';
  }
}
