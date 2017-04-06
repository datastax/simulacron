package com.datastax.simulacron.server;

import io.netty.channel.local.LocalAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.function.Supplier;

public interface AddressResolver extends Supplier<SocketAddress> {

  // TODO: make this configurable when needed.  For now we'll just use incrementing IPs from 127.0.1.1
  // but eventually it might be nice to have a resolver that returns incrementing ips + ports when C*
  // supports multiple instances per IP.  Also might be nice if a user wants to use a different IP range
  // or run multiple instances.
  AddressResolver defaultResolver = inet4Resolver(new byte[] {127, 0, 1, 1});

  AddressResolver localAddressResolver = () -> new LocalAddress(UUID.randomUUID().toString());

  /**
   * Creates a resolver that returns next unused IP address at port 9042. Starts from 127.0.0.1 and
   * cycles up to the next subnet as it goes, i.e. the order would go something like:
   *
   * <p>127.0.0.1, 127.0.0.2 ... 127.0.0.255, 127.0.1.1, 127.0.1.2 and so on.
   *
   * @param startingAddress IP to start from.
   * @return A resolver that returns incrementing ip addresses with port 9042.
   */
  static AddressResolver inet4Resolver(byte[] startingAddress) {
    // duplicate contents of input address so we don't mutate input.
    byte[] ipAddr = new byte[4];
    System.arraycopy(startingAddress, 0, ipAddr, 0, 4);

    // derives the next ip address to use.
    Supplier<InetAddress> nextAddr =
        () -> {
          try {
            InetAddress addr = InetAddress.getByAddress(ipAddr);

            // increment address
            for (int i = ipAddr.length - 1; i > 0; i--) {
              // roll over ipAddress if we max out the current octet (255)
              if (ipAddr[i] == (byte) 0xFF) {
                ipAddr[i] = 0;
              } else {
                ++ipAddr[i];
                break;
              }
            }
            return addr;
          } catch (UnknownHostException e) {
            // pretty safe to expect this won't happen.
            throw new UnsupportedOperationException(e);
          }
        };

    return () -> new InetSocketAddress(nextAddr.get(), 9042);
  }
}
