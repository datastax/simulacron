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
package com.datastax.oss.simulacron.standalone;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.beust.jcommander.JCommander;
import com.datastax.oss.simulacron.http.server.ActivityLogManager;
import com.datastax.oss.simulacron.http.server.ClusterManager;
import com.datastax.oss.simulacron.http.server.EndpointManager;
import com.datastax.oss.simulacron.http.server.HttpContainer;
import com.datastax.oss.simulacron.http.server.QueryManager;
import com.datastax.oss.simulacron.http.server.SwaggerUI;
import com.datastax.oss.simulacron.server.AddressResolver.Inet4Resolver;
import com.datastax.oss.simulacron.server.Server;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.LoggerFactory;

public class App {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(App.class);

  public static synchronized void main(String args[]) {
    // Disable the vertx dns resolver.  Since we only running against a local
    // endpoint, we don't need the benefit of vertx's async dns resolver which
    // doesn't load with newer netty versions anyways.
    System.setProperty("vertx.disableDnsResolver", "true");
    logger.info("Starting Simulacron.");
    CommandLineArguments cli = new CommandLineArguments();
    JCommander commander = new JCommander(cli, args);
    if (cli.help) {
      commander.usage();
      System.exit(-2);
    }

    // Adjust the root logger level based on configuration.
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    Logger datastaxLogger = (Logger) LoggerFactory.getLogger("com.datastax");
    if (cli.verbose) {
      root.setLevel(Level.DEBUG);
      datastaxLogger.setLevel(Level.DEBUG);
    } else {
      root.setLevel(cli.logLevel);
      datastaxLogger.setLevel(cli.logLevel);
    }

    HttpContainer httpServer = new HttpContainer(cli.httpInterface, cli.httpPort, cli.verbose);

    InetAddress ipAddress;
    try {
      ipAddress = InetAddress.getByName(cli.ipAddress);
      // Attempt to open up a socket on the interface
      new ServerSocket(0, 0, ipAddress);
    } catch (Exception e) {
      ipAddress = InetAddress.getLoopbackAddress();
      logger.error(
          "Failure binding on starting IP address to use for simulated C* nodes, defaulting to {}",
          ipAddress.getHostAddress(),
          e);
    }
    byte[] ipBytes = ipAddress.getAddress();
    Server nativeServer =
        Server.builder()
            .withAddressResolver(new Inet4Resolver(ipBytes))
            .withActivityLoggingEnabled(!cli.disableActivityLogging)
            .build();

    // TODO: There should probably be a module in http-server for setting up the http server instead of doing it here.
    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpServer.getRouter());

    QueryManager queryManager = new QueryManager(nativeServer);
    queryManager.registerWithRouter(httpServer.getRouter());

    EndpointManager endpointManager = new EndpointManager(nativeServer);
    endpointManager.registerWithRouter(httpServer.getRouter());

    ActivityLogManager logManager = new ActivityLogManager(nativeServer);
    logManager.registerWithRouter(httpServer.getRouter());

    SwaggerUI swaggerUI = new SwaggerUI();
    swaggerUI.registerWithRouter(httpServer.getRouter());

    // redirect the root to docs page.
    httpServer
        .getRouter()
        .route("/")
        .handler(
            context -> {
              context.response().putHeader("location", "/doc").setStatusCode(302).end();
            });

    try {
      httpServer.start().get(10, TimeUnit.SECONDS);
      logger.info(
          "Started HTTP server interface @ http://{}:{}.  Created nodes will start with ip {}.",
          cli.httpInterface,
          cli.httpPort,
          ipAddress);
      logger.info(
          "New to simulacron?  Visit http://{}:{}/doc for interactive API documentation.",
          cli.httpInterface,
          cli.httpPort);
      App.class.wait();
    } catch (ExecutionException e) {
      logger.error(
          "Failed to start HTTP server @ http://{}:{}, exiting.",
          cli.httpInterface,
          cli.httpPort,
          e.getCause());
      System.exit(-3);
    } catch (InterruptedException e) {
      logger.error("Interrupted while running HTTP server, exiting.", e);
    } catch (TimeoutException e) {
      logger.error("HTTP server did not start within 10 seconds, exiting.");
    }
  }
}
