package com.datastax.simulacron.driver;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.WriteType;

/**
 * Adapters for going to and from simulacron and driver types.
 *
 * <p>Since simulacron avoids a direct dependency to the driver it repeats some types that are in
 * the driver. For convenience this class maps between those types.
 */
public class DriverTypeAdapters {

  /**
   * Adapts a driver consistency level to simulacron consistency level.
   *
   * @param driverConsistency Input driver consistency level.
   * @return equivalent simulacron consistency level.
   */
  public static ConsistencyLevel adapt(
      com.datastax.driver.core.ConsistencyLevel driverConsistency) {
    return ConsistencyLevel.values()[driverConsistency.ordinal()];
  }

  /**
   * Extracts a driver consistency level from a simulacron consistency level.
   *
   * @param consistencyLevel Input simulacron consistency level.
   * @return equivalent driver consistency level.
   */
  public static com.datastax.driver.core.ConsistencyLevel extract(
      ConsistencyLevel consistencyLevel) {
    return com.datastax.driver.core.ConsistencyLevel.values()[consistencyLevel.ordinal()];
  }

  /**
   * Adapts a driver write type to simulacron write type.
   *
   * @param driverWriteType Input driver write type.
   * @return equivalent simulacron write type.
   */
  public static WriteType adapt(com.datastax.driver.core.WriteType driverWriteType) {
    return WriteType.values()[driverWriteType.ordinal()];
  }

  /**
   * Extracts a driver write type from a simulacron write type.
   *
   * @param writeType Input simulacron write type.
   * @return equivalent driver write type.
   */
  public static com.datastax.driver.core.WriteType extract(WriteType writeType) {
    return com.datastax.driver.core.WriteType.values()[writeType.ordinal()];
  }
}
