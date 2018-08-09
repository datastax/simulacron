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
package com.datastax.oss.simulacron.server;

import java.net.InetSocketAddress;
import java.util.Comparator;

public class InetSocketAddressComparator implements Comparator<InetSocketAddress> {

  public static InetSocketAddressComparator INSTANCE = new InetSocketAddressComparator();

  @Override
  public int compare(InetSocketAddress o1, InetSocketAddress o2) {
    // Compare ip addresses byte wise, i.e. 127.0.0.1 should come before 127.0.1.2
    byte[] o1Bytes = o1.getAddress().getAddress();
    byte[] o2Bytes = o2.getAddress().getAddress();

    // If comparing ipv6 and ipv4 addresses, consider ipv6 greater, this in practice
    // shouldn't happen.
    if (o1Bytes.length != o2Bytes.length) {
      return o1Bytes.length - o2Bytes.length;
    }

    // compare byte wise.
    for (int i = 0; i < o1Bytes.length; i++) {
      if (o1Bytes[i] != o2Bytes[i]) {
        return o1Bytes[i] - o2Bytes[i];
      }
    }

    // compare by port.
    return o1.getPort() - o2.getPort();
  }
}
