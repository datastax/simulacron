# Java API User's Guide

This document describes how to use the Simulacron Java API to provision and interact with simulated clusters.

For non-java development, it is recommended to use the [standalone jar](https://github.com/riptano/simulacron#getting-started-with-the-standalone-jar).

## Getting Simulacron

Simulacron can be added to your application by using the following maven dependency:

```xml
<dependency>
  <groupId>com.datastax.simulacron</groupId>
  <artifactId>native-server</artifactId>
  <version>0.2.0</version>
</dependency>
```

The native-server module provides all of the functionality needed to interact with simulacron, but if you are also
using the java driver you should consider depending on the `driver-3x` module which provides convenience mechanisms
(such as avoiding class name clashing) for working with the driver.  To install `driver-3x`:

```xml
<dependency>
  <groupId>com.datastax.simulacron</groupId>
  <artifactId>driver-3x</artifactId>
  <version>0.2.0</version>
</dependency>
```

## Creating a Server

[Server](../../native-server/src/main/java/com/datastax/simulacron/server/Server.java) provides a point of entry
into provisioning and de-provisioning simulated clusters.

To set up a server, simply do the following:

```java
import com.datastax.simulacron.server.Server;

class Test {
  static Server server = Server.builder().build();
}
```

Constructing a `Server` will register a netty `EventLoopGroup` and `Timer` under the covers.  Alternatively you may
provide your own `EventLoopGroup` as input to the builder.

By default nodes provisioned by Server will use socket addresses starting with 127.0.0.1:9042, 127.0.0.2:9042, and so
on.   To alter this behavior you may pass in a custom `AddressResolver` by using
`Server.Builder.withAddressResolver(AddressResolver)`.

`Server` is an [`AutoCloseable`][AutoCloseable] resource, thus can be used in a `try-with-resources` block or may be
closed explicitly using `close()`.

## Provisioning Clusters and Nodes

With a `Server` instance in hand, you can provision Cluster and Nodes using `Server.register`.

`register` returns `BoundCluster` and `BoundNode` instances which each implement
[`java.io.AutoCloseable`][AutoCloseable] and thus can be used in a `try-with-resources` block such that when the `try`
block` exits, the `Cluster` or `Node` is automatically unregistered with the `Server`.

To register a single `Node`:

```java
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.server.BoundNode;

try (BoundNode node = server.register(Node.builder())) {
    // interact with Node
}
```

To register a `Cluster`:

```java
import com.datastax.simulacron.server.BoundCluster;
import static com.datastax.simulacron.driver.SimulacronDriverSupport.cluster;

try (BoundCluster cluster = server.register(cluster().withNodes(10,10))) {
    // interact with Cluster
}
```

Note that the input `Cluster` and `Node` instances provided to `register` are ultimately different objects than those
returned.  The returned objects are 'bound' to the `Server`.

Also note the use of `cluster()`.  This is a convenience method that providers a `Cluster.Builder`.
This was introduced to avoid clashing with `com.datastax.driver.core.Cluster`.
If you aren't using the java driver, you could instead use `Cluster.builder()` to construct a Cluster.   Simulacron
provides additional conveniences to avoid namespace clashing with other driver types as well which is explained in the
[Driver Integration Support](#driver-integration-support) section.

## Cluster, DataCenter and Node configurability

`Cluster`, `DataCenter` and `Node`s can be configured in the following ways during construction with their builders:

All Subjects:

* `withCassandraVersion(String)` - Configures the version of C* to be configured.  Defaults to 3.0.12.
* `withDSEVersion(String)` - Configures the version of DSE to be configured.  If not present assumes not a DSE cluster.
If present adds some extra columns to peers table to mimic a DSE node.
* `withName(String)` - Gives a name to the subject.  Is used as Cluster and DC name in metadata respectively.
* `withPeerInfo(String key, Object value)` - Configures what peer columns should return.  Value types must match what
is expected or else defaults are used.
* `withPeerInfo(Map<String, Object> peerInfo)` - Full version of peer info.

`Cluster`:

* `withNodes(int nodeCount ...)` - Convenience for specifying DataCenter layout of Cluster with each index of input
representing a DC with number of nodes in that DC.

`Node`:

* `withAddress(SocketAddress address)` - Configure the listening address for the to be constructed Node.  If not
provided, the configured `AddressResolver` will assign an address automatically.

In the general case, configuring everything at the `Cluster` level is adequate, but in some cases you may want configure
`DataCenter` and `Node`s individually.  `DataCenter`s can be added to constructed `Cluster` instances using
`addDataCenter()`.  `Node`s can be added to constructed `DataCenter` instances using `addNode()`.  For example:

```java
import com.datastax.simulacron.common.cluster.*;
import java.util.UUID;

Cluster cluster = Cluster.builder().build();

// Add DC whose nodes are at C* 3.8.
DataCenter dc = cluster.addDataCenter().withCassandraVersion("3.8").build();

// Add nodes to dc with their own configuration.
Node node0 = dc.addNode().withPeerInfo("rack", "rack2").build();
Node node1 = dc.addNode().withPeerInfo("rack", "rack1").withPeerInfo("host_id", UUID.randomUUID()).build();

// Nodes and DataCenters cannot be added after the Cluster is registered to the Server.
BoundCluster bCluster = server.register(cluster);
```

This API is admittedly non-ideal.  See [#48](https://github.com/riptano/simulacron/issues/48) for future improvements.

## Shortcuts for accessing DataCenters and Nodes

Both `BoundCluster` and `BoundDataCenter` provide convenience methods to quickly accessing `DataCenter` and `Node`
objects respectively, i.e.:

```java
try (BoundCluster cluster = server.register(cluster().withNodes(5, 5, 5))) {
    // short cut to get a node in dc 0.
    BoundNode dc0node1 = cluster.node(1);
    // short cut to get dc 1.
    BoundNode dc1 = cluster.dc(1);
    // access dc 1, and then node 2 in that dc.
    BoundNode dc1node2 = dc1.node(2);
    // access node 3 in dc 2.
    BoundNode dc2node3 = cluster.dc(2, 3);
}
```

## Driver Integration Support

Simulacron includes driver-xx (i.e. `driver-3x`) compatibility modules for various versions of the java driver.
These modules are optional and merely provide convenience functionality.

### Simulacron Builders

`SimulacronDriverSupport` provides `cluster()` and `node()` for creating simulacron `Cluster` and `Node` builders
so there is no direct need to import them, i.e.:

```java
import static com.datastax.simulacron.driver.SimulacronDriverSupport.*;

BoundNode node = server.register(node());

BoundCluster cluster = server.register(cluster().withNodes(10));
```

### Building a Java Driver Cluster from a Simulacron Cluster

`SimulacronDriverSupport` also provides `defaultBuilder()` methods for creating
`com.datastax.driver.core.Cluster.Builder` instances that are preconfigured to communicate with simulacron `Cluster`s.

```java
import static com.datastax.simulacron.driver.SimulacronDriverSupport.*;

// Creates a builder that simply configures netty options such that it closes quickly.  This is useful
// for testing but has no intrinsic ties to simulacron functionality.
Cluster.Builder builder = defaultBuilder();

// Creates a builder that has its contact point pointing to the first node in the input cluster.
BoundCluster cluster = server.register(cluster().withNodes(3));
Cluster.Builder builder2 = defaultBuilder(cluster);

// Creates a builder that has its contact point pointing to the the input node.
Cluster.Builder builder3 = defaultBuilder(cluster.node(1));
```

### DriverTypeAdapters

`DriverTypeAdapters` offers methods for converting between types that share the same name and purpose.  Both the driver
and simulacron provide `Consistency` and `WriteType` implementations.

`adapt()` is meant to convert a driver type to a simulacron type.  `extract()` is meant to convert a simulacron type
to a driver type.  For example:

```java
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.WriteType;
import static com.datastax.simulacron.driver.DriverTypeAdapters.*;

// Convert from driver CL to simulacron CL
com.datastax.simulacron.common.codec.ConsistencyLevel cl = adapt(ConsistencyLevel.ONE);

// Convert from simulacron CL to driver CL
ConsistencyLevel driverCl = extract(cl);

// Convert from driver WriteType to simulacron WriteType
com.datastax.simulacron.common.codec.WriteType writeType = adapt(WriteType.SIMPLE);

// Convert from simulacron WriteType to simulacron WriteType
WriteType driverWriteType = extract(writeType);
```

## Priming Responses

The priming API provides a mechanism for defining how simulacron-simulated nodes should respond to requests.
Simulacron provides an easy to use fluent API,
[`PrimeDsl`](../../common/src/main/java/com/datastax/simulacron/common/stubbing/PrimeDsl.java), for priming queries.

A prime is broken up into two sections:

* `when`:  Defines the matching criteria, i.e.: When this query is made..
* `then`:  Defines what response to send, i.e.: Then send this response...

To begin a prime, simply use `PrimeDsl.when` to construct the when criteria, and then chain a `then` call with the
desired result.

For example, the following primes the query `select bar from foo` to return a 'read timeout' error that specifies that
0 out of 1 responses were received for a query requiring ONE consistency level:

```java
import com.datastax.simulacron.common.stubbing.PrimeDsl.PrimeBuilder;
import static com.datastax.simulacron.common.stubbing.PrimeDsl.*;

PrimeBuilder builder = when("select bar from foo").then(readTimeout(ConsistencyLevel.ONE, 0, 1, false));
Prime prime = builder.build();
```

To register the prime with a `BoundCluster`, `BoundDataCenter` or `BoundNode` simply call simply call `prime`.

```java
node.prime(prime);

// as a short cut, you could simply pass in the builder, i.e.:
cluster.prime(
        when("select bar from foo")
        .then(readTimeout(ConsistencyLevel.ONE, 0, 1, false)));
```


### Priming When (Requests)

`PrimeDsl` offers a few ways to prime `when` criteria:

* `when(String query)`: Creates a prime matching query string.
* `when(Request request)`: Creates a prime matching the input
[Request](../../common/src/main/java/com/datastax/simulacron/common/request/Request.java).

For more specific query criteria, the following can `query` methods can be used in conjunction with `when`:

* `query(String query, ConsistencyLevel consistency)`: Creates query criteria matching given query and consistency
level.
* `query(String query, List<ConsistencyLevel> consistencies)`: Creates query criteria matching given query with any of
the following consistencies.
* `query(String query, List<ConsistencyLevel> consistencies, Map<String, Object> params, Map<String, String> paramTypes)`:
Creates query criteria matching the given query with any of the following consistencies, having the following parameter
name, value mappings with the given name, value types.

In general, one will prime only queries.  However one can go as far as to configure responses for all kinds of requests.

For example, it may be desired to prime a node such that it does not respond to `Options` messages.  These are the
messages that a driver may send as a means of doing heartbeats.

To prime this scenario:

```java
import com.datastax.simulacron.common.request.Options;
import static com.datastax.simulacron.common.stubbing.PrimeDsl.*;

cluster.node(3).prime(when(Options.INSTANCE));
```

### Priming Then (Responses)

`PrimeDsl` offers a variety of response types that can be used as an input to `then`:

* `noRows()`: A successful response with 0 rows and no metadata.
* `rows()`: A successful response with the defined rows. See [Priming Row Responses](#priming-row-responses) for more
details.
* `alreadyExists(String keyspace, String table)`: An already exists exception for the given keyspace and table.
* `authenticationError(String message)`: An authentication error with the given message.
* `configurationError(String message)`: A configuration error with the given message.
* `closeConnection(DisconnectAction.Scope scope, CloseType closeType)`: Closes connections at the given scope with the
given close type.
* `functionFailure(String keyspace, String function, List<String> argTypes, String detail)`: A function failure for
the given function.
* `invalid(String message)`: An invalid error with the given message.
* `isBootstrapping()`: An error that indicates that the server is still bootstrapping.
* `overloaded(String message)`: An overloaded error with the given message.
* `readFailure(ConsistencyLevel cl, int received, int blockFor, Map failureReasonByEndpoint, boolean dataPresent)`: A
read failure error.
* `readTimeout(ConsistencyLevel cl, int received, int blockFor, boolean dataPresent)`: A read timeout error.
* `serverError(String message)`: A server error with the given message.
* `syntaxError(String message)`: A syntax error with the given message.
* `truncateError(String message)`: A truncate error with the given message.
* `unauthorized(String message)`: An unauthorized error with the given message.
* `unavailable(ConsistencyLevel cl, int required, int alive)`: An unavailable error.
* `unprepared(String message)`: An error indicating that the query was unprepared with the given message.
* `writeFailure(Consistencylevel cl, int required, int blockFor, Map failureReasonByEndpoint, WriteType writeType)`: A
write failure error.
* `writeTimeout(ConsistencyLevel cl, int received, int blockFor, WriteType writeType)`: A write timeout error.
* `void_()`: A 'void' result, which is the same result as having no prime, but is useful if you want to configure delays. 

In addition, you may simply not provide a `then`.  This indicates to simulacron to not respond to the given request.

### Priming Row Responses

A specialized builder is available for priming row responses as this would be arduous otherwise.  This is made
available via `PrimeDsl.rows`.

The following primes a response to 'select bar,baz from foo' to return 2 rows.

It is expected that the type names in `columnTypes` map to real cql types. Every cql type (included nested collections
and tuples) is supported with exception to UDTs.

```java
import static com.datastax.simulacron.common.stubbing.PrimeDsl.*;

cluster.node(1,2).prime(
        when("select bar,baz from foo")
        .then(rows()
            .row("bar", "hello", "baz", 72L)
            .row("bar", "world", "baz", 34L)
            .columnTypes("bar", "varchar", "baz", "bigint")
        ));
```

### Priming Delay

It may be desired to delay a response being sent by the server,  to do this, use
`PrimeBuilder.delay(long delay, TimeUnit delayUnit)`:

```java
import static com.datastax.simulacron.common.stubbing.PrimeDsl.*;

// delay response to query for 5 seconds.
cluster.prime(
        when("select bar,baz from foo")
        .then(noRows())
        .delay(5, TimeUnit.SECONDS)
);
```


## Using the Activity Log

By default simulacron will log all requests nodes received to an activity log for that node.  To disable activity
logging at the server level, one can simply do the following:

```java
Server server = Server.builder()
  .withActivityLoggingEnabled(false)
  .build();
```

One may also disable activity logging at the `Cluster` level when registering it:

```java
BoundCluster cluster = server.register(cluster().withNodes(5).build(),
  ServerOptions.builder().withActivityLoggingEnabled(false));

```

### Accessing Activity Logs

To access logs for a `BoundCluster`, `BoundDataCenter`, or `BoundServer` simply call `getLogs()`, i.e.:

```java
List<QueryLog> logs = node.getLogs();
```

`QueryLog` provides a variety of metadata about each query made on that node.

### Clearing Activity Logs

To clear activity logs, simply call `clearLogs()` on the target object you want to clear logs for, i.e.:

```java
dc.clearLogs();
```

## Accessing and Closing Connections

Simulacron offers away to retrieve the established socket connections by `Node` and further a way to close those
connections.

To access connections at a `Cluster`, `DataCenter`, or `Node` level, call `getConnections()`, i.e.:

```java
// cluster level
ClusterConnectionReport clusterReport = cluster.getConnections();
// dc level
DataCenterConnectionReport dcReport = cluster.dc(0).getConnections();
// node level
NodeConnectionReport nodeReport = cluster.node(0, 1).getConnections();
```

Depending on the subject, a `ConnectionReport` will be returned.  These objects are hierarchical in a similar structure
to `Cluster`, `DataCenter`, `Node`.  `NodeConnectionReport` offers a `getConnections()` method which returns all
established sockets to that node.

```java
List<SocketAddress> connections = nodeReport.getConnections();
```

To simply retrieve the number of active connections, one could use `getActiveConnections()`.

### Closing Connections

`closeConnections(CloseType closeType)` offers a mechanism to close all connections for a given subject, i.e.:

```java
import com.datastax.simulacron.common.stubbing.CloseType;

// Request to close connections and join until connections are closed.
// The connections that were closed are returned.
ClusterConnectionReport report = cluster.closeConnections(CloseType.DISCONNECT);
```

`CloseType` offers a number of ways by which to close a connection:
* `DISCONNECT` - Simply disconnects the connection.
* `SHUTDOWN_READ` - Prevents the socket from being able to read any inbound data.
Good for testing things like heartbeat failures.
* `SHUTDOWN_WRITE` - Prevents the socket from being able to write any outbound data.
This will typically close the connection so does not have much use over disconnect.

You can also close individual connections, i.e.:

```java
// Close the earliest established connection to the node.
NodeConnectionReport nodeReport = cluster.node(0, 1).getConnections();
cluster.closeConnection(nodeReport.get(0), CloseType.SHUTDOWN_READ);
```

## Disabling and Enabling Acceptance of Connections

Simulacron provides the capability to configure nodes to stop listening for new connections in addition to resuming
acceptance of connections.  This is useful for simulating node outages, or nodes being unresponsive for various reasons.

### Disabling Connection Listener

`Cluster`, `DataCenter` and `Node` each offer a `rejectConnections(int after, RejectScope rejectScope)` method for
telling nodes to disable listening for new connections, i.e.:

```java
import com.datastax.simulacron.server.RejectScope;

// simulate DC outage
cluster.dc(1).rejectConnections(0, RejectScope.STOP);

// alias for stopping immediately as done in previous instance
cluster.dc(1).stop().join();

// tell node to unbind listening for new connections after 1 successful connection while keeping existing connections open.
cluster.node(2).rejectConnections(1, RejectScope.UNBIND);

// tell cluster to continue accepting connections, but to not respond to 'STARTUP' requests.
// simulates scenario where network is responsive, but cassandra process is not.
cluster.rejectConnections(1, RejectScope.REJECT_STARTUP);
```

The `after` argument offers a way to tell simulacron to continue accepting connections until the provided number of
connections have been established.  This is useful for simulating partially initialized connection pools.

`RejectScope` offers a number of ways by which to reject connections:
* `UNBIND` - Stops listening for new connections.  Existing connections stay connected.
* `STOP` - Meant to simulate the behavior of stopping a node.  Closes existing connections and then unbinds to stop
listening for new ones.
* `REJECT_STARTUP` - Accepts new connections, but does not respond to 'startup' response.
Existing connections stay connected.  Meant to simulate behavior of when a cassandra process becomes unresponsive,
but the host OS is still able to read off the socket.

### Re-enabling Connection Listener

If node(s) previously had their connection listener disabled, they may be re-enabled by using `acceptConnections()`,
i.e.:

```java
// simulate DC outage
cluster.dc(1).stop();

// bring node 1 back up in dc 1.
cluster.node(1, 1).acceptConnections();

// start() is also provided as an alias to acceptConnections()
cluster.node(1, 1).start();
```

Note that if `acceptConnections()` is used on a subject that is already listening, there is no effect and the future
completes immediately.

## Async API

All methods that require network interactivity in `Server`, `BoundCluster`, `BoundDataCenter` and `BoundNode` have
asynchronous counterpart methods that are used under the covers and are also exposed in the API.  These methods have the
same name as their synchronous version, but ending with `Async`.  These methods return `CompletionStage`.  For example:

```java
// sync version
BoundCluster boundCluster = server.register(cluster().withNodes(5));

// async version
CompletionStage<BoundCluster> future = server.registerAsync(cluster().withNodes(5));
```

[AutoCloseable]: https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html
