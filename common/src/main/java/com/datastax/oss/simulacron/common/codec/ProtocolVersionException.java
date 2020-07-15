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

import com.datastax.oss.protocol.internal.response.result.RawType;

public class ProtocolVersionException extends InvalidTypeException {

  private final RawType rawType;
  private final int version;
  private final int minVersion;

  public ProtocolVersionException(RawType rawType, int version, int minVersion) {
    super(rawType + " requires protocol version " + minVersion + " but " + version + " is in use");
    this.rawType = rawType;
    this.version = version;
    this.minVersion = minVersion;
  }

  public RawType getType() {
    return rawType;
  }

  public int getVersion() {
    return version;
  }

  public int getMinVersion() {
    return minVersion;
  }
}
