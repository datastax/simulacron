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
import com.fasterxml.jackson.databind.JavaType;
import java.nio.ByteBuffer;

public interface Codec<T> {

  JavaType getJavaType();

  RawType getCqlType();

  ByteBuffer encode(T input);

  T decode(ByteBuffer input);

  T toNativeType(Object input);

  default ByteBuffer encodeObject(Object input) {
    return encode(toNativeType(input));
  }
}
