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
package com.datastax.oss.simulacron.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

public class NodePerPortResolverTest {

  @Test
  public void testNodeAssignedPerPort() {
    InetAddress startingAddress = Inet4Resolver.inetAddress(AddressResolver.defaultStartingIp);
    int expectedPort = 1000;
    AddressResolver resolver = new NodePerPortResolver(expectedPort);

    for (int i = 0; i < 1000; i++) {
      InetSocketAddress address = (InetSocketAddress) resolver.get();
      assertThat(address.getAddress()).isEqualTo(startingAddress);
      assertThat(address.getPort()).isEqualTo(expectedPort++);
    }
  }

  @Test
  public void testShouldCycleIpWhenPortsExhausted() {
    InetAddress expectedAddress = Inet4Resolver.inetAddress(AddressResolver.defaultStartingIp);
    int expectedPort = 65530;
    AddressResolver resolver = new NodePerPortResolver(expectedPort);

    for (int i = 0; i < 1000; i++) {
      InetSocketAddress address = (InetSocketAddress) resolver.get();
      assertThat(address.getAddress()).isEqualTo(expectedAddress);
      assertThat(address.getPort()).isEqualTo(expectedPort++);
      if (expectedPort == 65536) {
        expectedPort = 65530;
        expectedAddress =
            Inet4Resolver.inetAddress(Inet4Resolver.nextAddressBytes(expectedAddress.getAddress()));
      }
    }
  }
}
