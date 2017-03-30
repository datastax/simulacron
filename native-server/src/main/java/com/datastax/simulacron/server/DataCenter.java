package com.datastax.simulacron.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DataCenter {

  private static Logger logger = LoggerFactory.getLogger(DataCenter.class);

  private AtomicInteger nodeCounter = new AtomicInteger(1);

  private final Map<Integer, Node> nodes = new HashMap<>();

  private Cluster cluster;

  private final String name;

  public DataCenter(Cluster cluster, String name) {
    this.cluster = cluster;
    this.name = name;
  }

  public Node add() {
    int id = nodeCounter.getAndIncrement();
    String nodeName = name + ":" + id;
    SocketAddress address;
    try {
      address = new InetSocketAddress(IPResolver.next(), 9042);
    } catch (UnknownHostException e) {
      e.printStackTrace();
      address = null;
    }
    logger.debug("Assigning address {} to {}", address, nodeName);
    Node node = new Node(address, this, nodeName);
    nodes.put(id, node);
    return node;
  }

  public Node node(int i) {
    return nodes.get(i);
  }

  static class IPResolver {
    static byte[] ipAddr = new byte[] {127, 0, 0, 0};

    /**
     * Return next unused IP address. Starts from 127.0.0.1 and cycles up to the next subnet as it
     * goes, i.e. the order would go something like:
     *
     * <p>127.0.0.1, 127.0.0.2 ... 127.0.0.255, 127.0.1.1, 127.0.1.2 and so on.
     *
     * @return The next unused IP address.
     * @throws UnknownHostException
     */
    static synchronized InetAddress next() throws UnknownHostException {
      for (int i = ipAddr.length - 1; i > 0; i--) {
        if (ipAddr[i] == (byte) 0xFE) {
          ipAddr[i] = 0;
        } else {
          ++ipAddr[i];
          return InetAddress.getByAddress(ipAddr);
        }
      }
      return null;
    }
  }
}
