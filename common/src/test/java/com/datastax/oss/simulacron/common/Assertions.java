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

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.result.Rows;
import java.nio.ByteBuffer;

public class Assertions extends org.assertj.core.api.Assertions {

  public static MessageAssert assertThat(Message message) {
    return new MessageAssert(message);
  }

  public static RowsAssert assertThat(Rows rows) {
    return new RowsAssert(rows);
  }

  public static ByteBufferAssert assertThat(ByteBuffer buf) {
    return new ByteBufferAssert(buf);
  }
}
