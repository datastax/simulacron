package com.datastax.simulacron.common.stubbing;

import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.datastax.simulacron.common.codec.WriteType;
import com.datastax.simulacron.common.request.Options;
import com.datastax.simulacron.common.request.Query;
import com.datastax.simulacron.common.request.Request;
import com.datastax.simulacron.common.result.*;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A DSL that enables making {@link Prime}s more convenient. Example:
 *
 * <p><code>
 *      Prime prime = Prime.when("select * from tbl")
 *       .then(writeTimeout(ConsistencyLevel.ONE, 0, 1, WriteType.SIMPLE))
 *       .delay(1, TimeUnit.SECONDS)
 *       .forNode(node)
 *       .build();
 *  </code>
 */
public class PrimeDsl {

  /**
   * A fluent builder for constructing a prime, example usage:
   *
   * <p><code>
   *     Prime prime = Prime.when("select * from tbl")
   *       .then(writeTimeout(ConsistencyLevel.ONE, 0, 1, WriteType.SIMPLE))
   *       .delay(1, TimeUnit.SECONDS)
   *       .forNode(node)
   *       .build();
   * </code>
   *
   * @param when The {@link Request} to match against.
   * @return builder for this prime.
   */
  public static PrimeBuilder when(Request when) {
    return new PrimeBuilder(when);
  }

  /**
   * Convenience fluent builder for constructing a prime with a query.
   *
   * @param query The query string to match against.
   * @return builder for this prime.
   */
  public static PrimeBuilder when(String query) {
    return when(new Query(query));
  }

  /** Provides an 'Options' request instance. */
  public static Options options = Options.INSTANCE;

  /**
   * Provides a 'Query' request instance.
   *
   * @param query The query string.
   * @param consistencies The consistency to match on, or empty list to match on all.
   * @param params The parameters to match on.
   * @param paramTypes The types of the parameters.
   * @return a query instance.
   */
  public static Query query(
      String query,
      List<ConsistencyLevel> consistencies,
      Map<String, Object> params,
      Map<String, String> paramTypes) {
    return new Query(query, consistencies, params, paramTypes);
  }

  /**
   * Provides a Rows result with no rows.
   *
   * @return A rows response with no rows.
   */
  public static SuccessResult noRows() {
    return new SuccessResult(null, null);
  }

  /**
   * Provides a Rows result instance.
   *
   * @param rows The content of the rows, with each item in list representing a row as a map keyed
   *     by column name with it's value.
   * @param columnTypes The column name to it's type.
   * @return A rows response instance.
   */
  public static SuccessResult rows(
      List<Map<String, Object>> rows, Map<String, String> columnTypes) {
    return new SuccessResult(rows, columnTypes);
  }

  /**
   * A convenience method for quickly building rows results, example:
   *
   * <p><code>
   * server.prime(when("select * from data").then(rows()
   *  .row("column1", "hello", "column2", 2)
   *  .row("column1", "world", "column2", 4)
   *  .columnTypes("column1", "ascii", "column2", "bigint")
   *  .build();
   * </code>
   *
   * @return a builder used to construct a rows result
   */
  public static RowBuilder rows() {
    return new RowBuilder();
  }

  /**
   * Provides an Already exists error response instance.
   *
   * @param keyspace The keyspace for the error.
   * @param table The table for the error.
   * @return the generated error result.
   */
  public static AlreadyExistsResult alreadyExists(String keyspace, String table) {
    return new AlreadyExistsResult("unused", keyspace, table);
  }

  /**
   * Provides an Authentication error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static AuthenticationErrorResult authenticationError(String message) {
    return new AuthenticationErrorResult(message);
  }

  /**
   * Provides a Configuration error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static ConfigurationErrorResult configurationError(String message) {
    return new ConfigurationErrorResult(message);
  }

  /**
   * Provides a response type that closes the connection as a result of receiving the primed
   * request.
   *
   * @param scope The scope (connection, node, data center, cluster) at which to close the
   *     associated connection(s).
   * @param closeType The way to close the connection(s).
   * @return the generated result.
   */
  public static CloseConnectionResult closeConnection(
      DisconnectAction.Scope scope, DisconnectAction.CloseType closeType) {
    return new CloseConnectionResult(scope, closeType, 0);
  }

  /**
   * Provides a Function Failure error response instance.
   *
   * @param keyspace the keyspace the function belongs to
   * @param function the function name
   * @param argTypes the argument types (cql types) for the function
   * @param detail detail of the failure
   * @return the generated error result.
   */
  public static FunctionFailureResult functionFailure(
      String keyspace, String function, List<String> argTypes, String detail) {
    return new FunctionFailureResult(keyspace, function, argTypes, detail);
  }

  /**
   * Provides an Invalid error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static InvalidResult invalid(String message) {
    return new InvalidResult(message);
  }

  /**
   * Provides an Is Bootstrapping error response instance.
   *
   * @return the generated error result.
   */
  public static IsBootstrappingResult isBootstrapping() {
    return new IsBootstrappingResult();
  }

  /**
   * Provides an Overloaded error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static OverloadedResult overloaded(String message) {
    return new OverloadedResult(message);
  }

  /**
   * Provides an Protocol error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static ProtocolErrorResult protocolError(String message) {
    return new ProtocolErrorResult(message);
  }

  /**
   * Provides a Read Failure error response instance.
   *
   * @param cl The consistency level data was read at.
   * @param received How many responses were received from replicas.
   * @param blockFor How many responses were required from replicas.
   * @param failureReasonByEndpoint The failures by node with their failure message.
   * @param dataPresent Whether or not data was read.
   * @return the generated error result.
   */
  public static ReadFailureResult readFailure(
      ConsistencyLevel cl,
      int received,
      int blockFor,
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint,
      boolean dataPresent) {
    return new ReadFailureResult(cl, received, blockFor, failureReasonByEndpoint, dataPresent);
  }

  /**
   * Provides a Read Timeout error response instance.
   *
   * @param cl The consistency level data was read at.
   * @param received How many responses were received from replicas.
   * @param blockFor How many responses were required from replicas.
   * @param dataPresent Whether or not data was read.
   * @return the generated error result.
   */
  public static ReadTimeoutResult readTimeout(
      ConsistencyLevel cl, int received, int blockFor, boolean dataPresent) {
    return new ReadTimeoutResult(cl, received, blockFor, dataPresent);
  }

  /**
   * Provides an Server error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static ServerErrorResult serverError(String message) {
    return new ServerErrorResult(message);
  }

  /**
   * Provides an Syntax error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static SyntaxErrorResult syntaxError(String message) {
    return new SyntaxErrorResult(message);
  }

  /**
   * Provides an Truncate error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static TruncateErrorResult truncateError(String message) {
    return new TruncateErrorResult(message);
  }

  /**
   * Provides an Unauthorized error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static UnauthorizedResult unauthorized(String message) {
    return new UnauthorizedResult(message);
  }

  /**
   * Provides an Unavailable error response instance.
   *
   * @param cl The consistency level used for the request.
   * @param required How many replicas were required to meet consistency.
   * @param alive How many replicas are alive.
   * @return the generated error result.
   */
  public static UnavailableResult unavailable(ConsistencyLevel cl, int required, int alive) {
    return new UnavailableResult(cl, required, alive);
  }

  /**
   * Provides an Unprepared error response instance.
   *
   * @param message The message to include with the error.
   * @return the generated error result.
   */
  public static UnpreparedResult unprepared(String message) {
    return new UnpreparedResult(message);
  }

  /**
   * Provides a Read Failure error response instance.
   *
   * @param cl The consistency level data was written at.
   * @param received How many responses were received from replicas.
   * @param blockFor How many responses were required from replicas.
   * @param failureReasonByEndpoint The failures by node with their failure message.
   * @param writeType The kind of write that failed.
   * @return the generated error result.
   */
  public static WriteFailureResult writeFailure(
      ConsistencyLevel cl,
      int received,
      int blockFor,
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint,
      WriteType writeType) {
    return new WriteFailureResult(cl, received, blockFor, failureReasonByEndpoint, writeType);
  }

  /**
   * Provides a Write Timeout error response instance.
   *
   * @param cl The consistency level data was written at.
   * @param received How many responses were received from replicas.
   * @param blockFor How many responses were required from replicas.
   * @param writeType The kind of write that resulted in timeout.
   * @return the generated error result.
   */
  public static WriteTimeoutResult writeTimeout(
      ConsistencyLevel cl, int received, int blockFor, WriteType writeType) {
    return new WriteTimeoutResult(cl, received, blockFor, writeType);
  }

  public static class PrimeBuilder {
    private Request when;
    private Result then;
    private Scope scope;

    PrimeBuilder(Request when) {
      this.when = when;
    }

    /**
     * @param then the result to return for the prime
     * @return this builder
     */
    public PrimeBuilder then(Result then) {
      this.then = then;
      return this;
    }

    public PrimeBuilder delay(long delay, TimeUnit delayUnit) {
      if (then == null) {
        throw new RuntimeException("then must be called before delay.");
      }
      then.setDelay(delay, delayUnit);
      return this;
    }

    /**
     * Indicates that the prime should only be applied to this cluster.
     *
     * @param cluster Cluster to apply prime to.
     * @return this builder.
     */
    public PrimeBuilder forCluster(Cluster cluster) {
      return forTopic(cluster);
    }

    /**
     * Indicates that the prime should only be applied to this data center.
     *
     * @param dc DataCenter to apply prime to.
     * @return this builder.
     */
    public PrimeBuilder forDataCenter(DataCenter dc) {
      return forTopic(dc);
    }

    /**
     * Indicates that the prime should only be applied to this node.
     *
     * @param node Node to apply prime to.
     * @return this builder.
     */
    public PrimeBuilder forNode(Node node) {
      return forTopic(node);
    }

    /**
     * @param topic The node, dc, or cluster to apply this prime to.
     * @return this builder.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private PrimeBuilder forTopic(AbstractNodeProperties topic) {
      Scope scope;
      if (topic instanceof Cluster) {
        scope = new Scope(topic.getId(), null, null);
      } else if (topic instanceof DataCenter) {
        scope = new Scope(topic.getParent().get().getId(), topic.getId(), null);
      } else { // assume node
        scope =
            new Scope(
                topic.getParent().get().getParent().get().getId(),
                topic.getParent().get().getId(),
                topic.getId());
      }
      this.scope = scope;
      return this;
    }

    /** @return a {@link Prime} from this configuration. */
    public Prime build() {
      return new Prime(new RequestPrime(when, then), scope);
    }
  }

  public static class RowBuilder {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, String> columnTypes;

    public RowBuilder row(Object... data) {
      Map<String, Object> rowData = new HashMap<>();
      if (data.length % 2 != 0) {
        throw new RuntimeException(
            "Expected row data to contain an even number of values, but was given " + data.length);
      }
      for (int i = 0; i < data.length - 1; i += 2) {
        rowData.put(data[i].toString(), data[i + 1]);
      }
      rows.add(rowData);

      return this;
    }

    public RowBuilder columnTypes(String... columns) {
      if (columns.length % 2 != 0) {
        throw new RuntimeException(
            "Expected row data to contain an even number of values, but was given "
                + columns.length);
      }
      columnTypes = new HashMap<>();
      for (int i = 0; i < columns.length - 1; i += 2) {
        columnTypes.put(columns[i], columns[i + 1]);
      }

      return this;
    }

    public SuccessResult build() {
      return new SuccessResult(rows, columnTypes);
    }
  }
}
