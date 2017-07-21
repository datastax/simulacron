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
package com.datastax.oss.simulacron.common;

import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import org.assertj.core.api.AbstractAssert;

public class ByteBufferAssert extends AbstractAssert<ByteBufferAssert, ByteBuffer> {

  protected ByteBufferAssert(ByteBuffer actual) {
    super(actual, ByteBufferAssert.class);
  }

  public ByteBufferAssert hasBytes(String expected) {
    String byteStr = Bytes.toHexString(actual);

    org.assertj.core.api.Assertions.assertThat(byteStr).isEqualTo(expected);
    return this;
  }
}
