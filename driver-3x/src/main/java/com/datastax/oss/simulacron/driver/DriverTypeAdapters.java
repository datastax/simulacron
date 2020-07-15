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

import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;

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
