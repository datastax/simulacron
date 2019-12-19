# Simulacron - An Apache Cassandra® Native Protocol Server Simulator

[![Build Status](https://travis-ci.org/datastax/simulacron.svg?branch=master)](https://travis-ci.org/datastax/simulacron)

A native protocol server simulator that helps facilitate the testing of scenarios that are difficult to reliably
reproduce in driver clients and applications.

Inspired by [Scassandra](https://scassandra.org), simulacron is a pure java implementation with increased
emphasis on testing with many simulated native protocol endpoints.

## Features

* **[Java API](doc/java_api)** - For creating and interacting with simulated Clusters.
* **[Standalone JAR](#getting-started-with-the-standalone-jar)** - HTTP JSON API for creating and interacting with
  clusters.
* **Interactive Documentation** - [Swagger](http://swagger.io)-based doc for exploring and making API calls.
* **Lightweight implementation** - Uses [netty](http://netty.io).  Can spawn many simulated nodes on local ip addresses.
  Simulating multi-thousand node clusters is fast and takes very little system resources.
* **Peer Discovery support for Multiple Drivers** - Responds to discovery queries made by DataStax java, csharp,
  c++, php and python drivers with more to come.  Capability to add support for other drivers if needed.
* **Protocol Version V3+ support** - Uses the [native-protocol][native-protocol] library for encoding/decoding.
* **Activity Logging** - Logs requests by clients for each node.
* **Priming Interface** - Configure node behaviors for handling certain requests.
* **Connection API** - Programmatic accessing and closing of client sockets and simulation of starting, stopping
  nodes in addition to making them unresponsive in various ways.

## Prerequisites

0. Java 8+ - Simulacron is a Java-based application built on Java 8.
1. [Apache Maven](https://maven.apache.org) 3.3+ - For building the project.
2. **MacOS only**:  To be able to define more than a single node cluster, multiple loopback aliases should be added.

   This is not required on Linux or Windows since these are implicitly defined.  The following script will add
   127.0.0.0-127.0.4.255:

   ```bash
   #!/bin/bash
   for sub in {0..4}; do
       echo "Opening for 127.0.$sub"
       for i in {0..255}; do sudo ifconfig lo0 alias 127.0.$sub.$i up; done
   done
   ```

   Note that this is known to cause temporary increased CPU usage in OS X initially while mDNSResponder acclimates
   itself to the presence of added ip addresses.  This lasts several minutes.

   Also note that on reboot these ip addresses need to be re-added.

## Getting Started with the Standalone Jar

To use simulacron with its included HTTP server, one may use the standalone jar.

Pre-built versions of the standalone jar can be downloaded from the
[releases page](https://github.com/datastax/simulacron/releases).

The jar is executed in the following manner:

```bash
java -jar simulacron-standalone-<VERSION>.jar
```

If you'd like to build simulacron, follow these instructions:

0. Set up your maven settings.xml to use artifactory ([see java-api doc](doc/java_api#getting-simulacron))
1. `mvn package` - Compiles, Tests, and packages the project.  Produces standalone jar in `standalone/target/`
2. `java -jar standalone/target/simulacron-standalone-<VERSION>.jar` -  Runs the standalone app.
3. Navigate to [http://localhost:8187/doc](http://localhost:8187/doc) to access the interactive documentation.

### Usage

```
Usage:
  Options:
    --disable-activity-log, -d
      Disables activity logging by default
      Default: false
    --httpintf, -t
      Interface address to bind HTTP server on
      Default: localhost
    --httpport, -p
      HTTP port to bind on
      Default: 8187
    --ip, -i
      Starting IP address to create simulated C* nodes on
      Default: 127.0.0.1
    --loglevel, -l
      Logging level to use
      Default: INFO
    --starting-port, -s
      Starting Port to assign Nodes to.  Note that if this is used multiple
      nodes can be assigned on one IP (which mimics C* 4.0+ peering)
      Default: -1
    --verbose, -v
      Whether or not to enable verbose http logging (also enables DEBUG
      logging)
      Default: false
```

## Using the Java API

As simulacron is a java project, it includes a Java API that bypasses the need to use the HTTP interface all together.

See the [Java API User's Guide](doc/java_api) for using this API.

## License

© DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

----

DataStax is a registered trademark of DataStax, Inc. and its subsidiaries in the United States 
and/or other countries.

Apache Cassandra, Apache, Tomcat, Lucene, Solr, Hadoop, Spark, TinkerPop, and Cassandra are 
trademarks of the [Apache Software Foundation](http://www.apache.org/) or its subsidiaries in
Canada, the United States and/or other countries. 

[native-protocol]: https://github.com/datastax/native-protocol
