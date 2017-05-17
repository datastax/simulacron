# Simulacron - An Apache Cassandra Native Protocol Server Simulator

A native protocol server simulator that helps facilitate testing more difficult to reliably produce scenarios in driver
clients and applications.

Inspired by [Scassandra](https://scassandra.org), simulacron is a pure java implementation with increased
emphasis on testing with many simulated native protocol endpoints.

## Features

* **Java API** for creating and interacting with simulated Clusters.
* **Standalone JAR** with an admin HTTP JSON API for creating and interacting with clusters.
* **Interactive Documentation**provided with standalone jar for exploring APIs.  Uses [swagger](http://swagger.io).
* **Lightweight implementation** that uses [netty](http://netty.io).  Can spawn many simulated nodes behind a listening
  socket.  Simulating multi-thousand node clusters takes very little system resources.
* **Peer Discovery support for Multiple Drivers**.  Responds to discovery queries made by DataStax java, csharp, 
  c++, php and python drivers with more to come.
* **Protocol Version V3+ support**
* **Activity Logging** logs requests by clients for each node.
* **Stubbing Interface** configure node behaviors for handling certain requests.

## Prerequisites

0. Java 8+ - Simulacron is a Java-based application built on Java 8.
1. [Apache Maven](https://maven.apache.org) 3.3+ - For building the project.
2. [native-protocol](https://github.com/riptano/native-protocol) - Provides encoding/decoding of the native protocol.
   As this project is not currently available on maven central it needs to be built and installed locally, to do so:

   ```
   git clone git@github.com:riptano/native-protocol.git
   cd native-protocol
   mvn clean install
   ```
4. **MacOS only**:  To be able to define more than a single node cluster, multiple loopback aliases should be added.

   This is not required on Linux or Windows since these are implicitly defined.  The following script will add
   127.0.0.0-127.0.4.255:

   ```bash
   #!/bin/bash
   for sub in {0..4}; do
       echo "Opening for 127.0.$sub"
       for i in {0..255}; do sudo ifconfig lo0 alias 127.0.$sub.$i up; done
   done
   ```

## Getting Started

To build and run simulacron follow these instructions:

1. `mvn package` - Compiles, Tests, and packages the project.  Produces standalone jar in `standalone/target/`
2. `java -jar standalone/target/standalone-<VERSION>.jar` -  Runs the standalone app.
3. Navigate to [http://localhost:8187/doc](http://localhost:8187/doc) to access the interactive documentation.

### Usage

```
Usage:
  Options:
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
    --verbose, -v
      Whether or not to enable verbose http logging (also enables DEBUG
      logging)
      Default: false
```
