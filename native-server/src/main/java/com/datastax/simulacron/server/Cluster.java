package com.datastax.simulacron.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class Cluster {

  private Server server;

  private AtomicInteger dcCounter = new AtomicInteger(1);

  private Map<Integer, DataCenter> dataCenters = new HashMap<>();

  public Cluster(Server server) {
    this.server = server;
  }

  public DataCenter addDataCenter() {
    int id = dcCounter.getAndIncrement();
    String name = "dc" + id;
    DataCenter dc = new DataCenter(this, name);
    dataCenters.put(id, dc);
    return dc;
  }

  public DataCenter dc(int i) {
    return dataCenters.get(i);
  }

  CompletableFuture<Void> bind(Node node) {
    return server.bind(node);
  }

  public CompletableFuture<Void> init(Integer... nodesInDc) {
    List<CompletableFuture<Node>> futures = new ArrayList<>();
    for (Integer nodeCount : nodesInDc) {
      DataCenter dc = addDataCenter();
      for (int i = 0; i < nodeCount; i++) {
        futures.add(dc.addNode());
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
        .thenApply(v -> null);
  }

  public static void main(String args[]) {
    // Start a simulated Cluster with 3 DCs with 10 nodes in each DC.
    Cluster cluster = new Cluster(new Server());
    cluster.init(10, 10, 10);
  }
}
