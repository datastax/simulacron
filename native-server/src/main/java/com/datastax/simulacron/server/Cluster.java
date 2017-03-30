package com.datastax.simulacron.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class Cluster {

  private AtomicInteger dcCounter = new AtomicInteger(1);

  private Map<Integer, DataCenter> dataCenters = new HashMap<>();

  public Cluster() {}

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

  public CompletableFuture<Void> init(Server server, Integer... nodesInDc) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Integer nodeCount : nodesInDc) {
      DataCenter dc = addDataCenter();
      for (int i = 0; i < nodeCount; i++) {
        Node node = dc.add();
        futures.add(server.bind(node));
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
        .thenApply(v -> null);
  }

  public static void main(String args[]) {
    // Start a simulated Cluster with 3 DCs with 10 nodes in each DC.
    Server server = new Server();
    Cluster cluster = new Cluster();
    cluster.init(server, 10, 10, 10);
  }
}
