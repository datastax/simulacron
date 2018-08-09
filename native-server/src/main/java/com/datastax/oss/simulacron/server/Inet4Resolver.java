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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resolver that returns next unused IP address at port 9042. Starts from an input address and
 * cycles up to the next subnet as it goes, i.e. the order would go something like:
 *
 * <p>127.0.0.1, 127.0.0.2 ... 127.0.0.255, 127.0.1.1, 127.0.1.2 and so on.
 */
public class Inet4Resolver implements AddressResolver {

  private static final Logger logger =
      LoggerFactory.getLogger(com.datastax.oss.simulacron.server.Inet4Resolver.class);
  private final AtomicReference<byte[]> ipParts;
  private static final AtomicBoolean WARNED = new AtomicBoolean();
  private final int port;

  // Use priority queue so released addresses are sorted by ip address.
  private final Queue<InetSocketAddress> releasedAddresses =
      new PriorityBlockingQueue<>(10, InetSocketAddressComparator.INSTANCE);

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
    checkAddressPresence(ipAddr, 100);
  }

  public static void checkAddressPresence(byte ipBytes[], int numberAddresses) {
    if (WARNED.get()) {
      return;
    }

    // checks that the OS has 100 local ip addresses.
    // this check is really only needed for OS X, otherwise return.
    String osName = System.getProperty("os.name", "none");
    if (!osName.toLowerCase().startsWith("mac")) {
      return;
    }
    for (int i = 0; i < numberAddresses; i++) {
      InetAddress inetAddress = inetAddress(ipBytes);
      try {
        // Attempt to create socket on that address.
        new ServerSocket(0, 0, inetAddress);
      } catch (IOException e) {
        if (WARNED.compareAndSet(false, true)) {
          logger.warn(
              "Detected that {} is not available for use.  "
                  + "Refer to https://goo.gl/Ru7gUj for information on how to provision IPs on OS X.",
              inetAddress);
        }
        return;
      }
      ipBytes = nextAddressBytes(ipBytes);
    }
  }

  public static InetAddress inetAddress(byte[] ipBytes) {
    byte[] ipAddr = new byte[4];
    System.arraycopy(ipBytes, 0, ipAddr, 0, 4);

    InetAddress addr;
    try {
      addr = InetAddress.getByAddress(ipAddr);
    } catch (UnknownHostException uhe) {
      throw new IllegalArgumentException("Could not create ip address", uhe);
    }
    return addr;
  }

  public static byte[] nextAddressBytes(byte[] currentIpBytes) {
    byte[] newBytes = new byte[4];
    System.arraycopy(currentIpBytes, 0, newBytes, 0, 4);
    for (int i = currentIpBytes.length - 1; i > 0; i--) {
      // roll over ipAddress if we max out the current octet (255)
      if (newBytes[i] == (byte) 0xFE) {
        newBytes[i] = 0;
      } else {
        ++newBytes[i];
        break;
      }
    }
    return newBytes;
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
        byte[] ref = ipParts.get();
        InetAddress addr = inetAddress(ref);
        byte[] next = nextAddressBytes(ref);

        if (ipParts.compareAndSet(ref, next)) {
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
