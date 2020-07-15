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
package com.datastax.oss.simulacron.protocol.json;

public class SerializerUtils {

  public static String toConsistencyString(int consistency) {
    switch (consistency) {
      case 0:
        return "ANY";
      case 1:
        return "ONE";
      case 2:
        return "TWO";
      case 3:
        return "THREE";
      case 4:
        return "QUORUM";
      case 5:
        return "ALL";
      case 6:
        return "LOCAL_QUORUM";
      case 7:
        return "EACH_QUORUM";
      case 8:
        return "SERIAL";
      case 9:
        return "LOCAL_SERIAL";
      case 10:
        return "LOCAL_ONE";
      default:
        return "" + consistency;
    }
  }
}
