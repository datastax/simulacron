package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.ClusterMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {
  private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  private static final ObjectMapper om = ClusterMapper.getMapper();

  public static void handleError(ErrorMessage message, RoutingContext context) {
    if (message.getException() != null) {
      logger.error("Error encountered while handling request.", message);
    }
    handleMessage(message, context);
  }

  public static void handleMessage(Message message, RoutingContext context) {
    try {
      String msgJson = om.writerWithDefaultPrettyPrinter().writeValueAsString(message);
      context
          .request()
          .response()
          .putHeader("content-type", "application/json")
          .setStatusCode(message.getStatusCode())
          .end(msgJson);
    } catch (JsonProcessingException e) {
      context
          .request()
          .response()
          .putHeader("content-type", "application/json")
          .setStatusCode(500)
          .end("{\"message\": \"Internal Server Error, refer to logs\", \"status_code\": 500}");
    }
  }
}
