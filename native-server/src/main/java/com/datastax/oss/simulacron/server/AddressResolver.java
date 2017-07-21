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

import io.netty.channel.local.LocalAddress;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public interface AddressResolver extends Supplier<SocketAddress> {

  byte[] defaultStartingIp = new byte[] {127, 0, 1, 1};
  int defaultStartingPort = 9042;

  // TODO: make this configurable when needed.  For now we'll just use incrementing IPs from 127.0.1.1
  // but eventually it might be nice to have a resolver that returns incrementing ips + ports when C*
  // supports multiple instances per IP.  Also might be nice if a user wants to use a different IP range
  // or run multiple instances.
  AddressResolver defaultResolver = new Inet4Resolver();

  AddressResolver localAddressResolver = () -> new LocalAddress(UUID.randomUUID().toString());

  /**
   * A resolver that returns next unused IP address at port 9042. Starts from an input address and
   * cycles up to the next subnet as it goes, i.e. the order would go something like:
   *
   * <p>
   *
   * <p>
   *
   * <p>
   *
   * <p>127.0.0.1, 127.0.0.2 ... 127.0.0.255, 127.0.1.1, 127.0.1.2 and so on.
   */
  class Inet4Resolver implements AddressResolver {
    private final AtomicReference<byte[]> ipParts;
    private final int port;

    // Use priority queue so released addresses are sorted by ip address.
    Queue<InetSocketAddress> releasedAddresses =
        new PriorityBlockingQueue<>(
            10,
            (o1, o2) -> {
              // Compare ip addresses byte wise, i.e. 127.0.0.1 should come before 127.0.1.2
              byte[] o1Bytes = o1.getAddress().getAddress();
              byte[] o2Bytes = o2.getAddress().getAddress();

              // If comparing ipv6 and ipv4 addresses, consider ipv6 greater, this in practice shouldn't happen.
              if (o1Bytes.length != o2Bytes.length) {
                return o1Bytes.length - o2Bytes.length;
              }

              // compare byte wise.
              for (int i = 0; i < o1Bytes.length; i++) {
                if (o1Bytes[i] != o2Bytes[i]) {
                  return o1Bytes[i] - o2Bytes[i];
                }
              }

              // addresses are the same.
              return 0;
            });

    public Inet4Resolver(byte[] startingAddress) {
      this(startingAddress, defaultStartingPort);
    }

    public Inet4Resolver() {
      this(defaultStartingIp, defaultStartingPort);
    }

    public Inet4Resolver(int port) {
      this(defaultStartingIp, port);
    }

    public Inet4Resolver(byte startingAddress[], int port) {
      byte[] ipAddr = new byte[4];

      System.arraycopy(startingAddress, 0, ipAddr, 0, 4);
      this.ipParts = new AtomicReference<>(ipAddr);
      this.port = port;
    }

    @Override
    public SocketAddress get() {
      // If an address was released, reuse it.
      InetSocketAddress address = releasedAddresses.poll();
      if (address != null) {
        return address;
      } else {
        // get current address and increment to create next one.
        // if CAS fails, try again until it works.
        while (true) {
          byte[] ipAddr = new byte[4];
          byte[] ref = ipParts.get();
          System.arraycopy(ref, 0, ipAddr, 0, 4);

          InetAddress addr = null;
          try {
            addr = InetAddress.getByAddress(ipAddr);
          } catch (UnknownHostException uhe) {
            throw new IllegalArgumentException("Could not create ip address", uhe);
          }

          for (int i = ipAddr.length - 1; i > 0; i--) {
            // roll over ipAddress if we max out the current octet (255)
            if (ipAddr[i] == (byte) 0xFE) {
              ipAddr[i] = 0;
            } else {
              ++ipAddr[i];
              break;
            }
          }
          if (ipParts.compareAndSet(ref, ipAddr)) {
            return new InetSocketAddress(addr, this.port);
          }
        }
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void release(SocketAddress address) {
      releasedAddresses.offer((InetSocketAddress) address);
    }
  }

  /**
   * Indicates to the resolver that the input address that was previously generated by it is no
   * longer in use and may be reused.
   *
   * @param address Address to return.
   */
  default void release(SocketAddress address) {}
}
