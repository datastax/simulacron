/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

public enum ConsistencyLevel {
  ANY(0),
  ONE(1),
  TWO(2),
  THREE(3),
  QUORUM(4),
  ALL(5),
  LOCAL_QUORUM(6),
  EACH_QUORUM(7),
  SERIAL(8),
  LOCAL_SERIAL(9),
  LOCAL_ONE(10);

  private final int code;

  private ConsistencyLevel(int code) {
    this.code = code;
  }

  public static ConsistencyLevel fromString(String text) {
    for (ConsistencyLevel cl : ConsistencyLevel.values()) {
      if (cl.name().equalsIgnoreCase(text)) {
        return cl;
      }
    }
    return null;
  }

  public static ConsistencyLevel fromCode(int code) {
    for (ConsistencyLevel cl : ConsistencyLevel.values()) {
      if (cl.code == code) {
        return cl;
      }
    }
    return null;
  }

  public int getCode() {
    return code;
  }
}
