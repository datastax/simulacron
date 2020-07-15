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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A resolver that returns the next unused port for the current ip address. When we've reached the
 * end of a port range on an ip, the next ip is used starting with the input starting port. The
 * order would look something like:
 *
 * <p>127.0.0.1:49152,127.0.0.1:49153,...,127.0.0.1:65535,127.0.0.2:49152 and so on.
 */
public class NodePerPortResolver implements AddressResolver {

  // the next ip address to use
  private final AtomicReference<InetSocketAddress> ip;
  // the port to use at start of ip range
  private final int startingPortPerIp;

  // Use priority queue so released addresses are sorted by ip address and port.
  private final Queue<InetSocketAddress> releasedAddresses =
      new PriorityBlockingQueue<>(10, InetSocketAddressComparator.INSTANCE);

  public NodePerPortResolver() {
    this(defaultStartingIp, 49152);
  }

  public NodePerPortResolver(int startingPortPerIp) {
    this(defaultStartingIp, startingPortPerIp);
  }

  public NodePerPortResolver(byte startingAddress[], int startingPortPerIp) {
    byte[] ipAddr = new byte[4];

    System.arraycopy(startingAddress, 0, ipAddr, 0, 4);
    this.ip =
        new AtomicReference<>(
            new InetSocketAddress(Inet4Resolver.inetAddress(ipAddr), startingPortPerIp));

    this.startingPortPerIp = startingPortPerIp;

    Inet4Resolver.checkAddressPresence(ipAddr, 1);
  }

  @Override
  public SocketAddress get() {
    // If an address was released, reuse it.
    InetSocketAddress address = releasedAddresses.poll();
    if (address != null) {
      return address;
    } else {
      // get current address and increment to the next one.
      // if CAS fails try again until it works.
      while (true) {
        InetSocketAddress addr = ip.get();

        int port = addr.getPort() + 1;

        InetSocketAddress newAddress;
        // if we've exhausted ports, move on to next ip, otherwise use next port.
        if (port > 65535) {
          newAddress =
              new InetSocketAddress(
                  Inet4Resolver.inetAddress(
                      Inet4Resolver.nextAddressBytes(addr.getAddress().getAddress())),
                  startingPortPerIp);
        } else {
          newAddress = new InetSocketAddress(addr.getAddress(), port);
        }

        if (ip.compareAndSet(addr, newAddress)) {
          return addr;
        }
      }
    }
  }
}
