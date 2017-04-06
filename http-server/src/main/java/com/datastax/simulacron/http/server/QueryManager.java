package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.Query;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class QueryManager implements HttpListener {
  Logger logger = LoggerFactory.getLogger(QueryManager.class);
  public static ConcurrentHashMap<String, Query> queries = new ConcurrentHashMap<String, Query>();

  public void primerQuery(RoutingContext context) {

    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              System.out.println("Full body received, length = " + totalBuffer.length());
              String jsonBody = totalBuffer.toString();
              ObjectMapper om = new ObjectMapper();
              om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
              try {
                Query query = om.readValue(jsonBody, Query.class);
                queries.put(query.when.query, query);

              } catch (IOException e) {
                logger.error("Error decoding json cluster object encountered", e);
              }
            });
    context.request().response().end("Invoking cluster creation");
  }

  public void registerWithRouter(Router router) {
    router.route(HttpMethod.POST, "/prime*").handler(this::primerQuery);
  }
}
