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
package com.datastax.oss.simulacron.common.stubbing;

import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.RequestFailureReason;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.request.Options;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.request.Request;
import com.datastax.oss.simulacron.common.result.AlreadyExistsResult;
import com.datastax.oss.simulacron.common.result.AuthenticationErrorResult;
import com.datastax.oss.simulacron.common.result.CloseConnectionResult;
import com.datastax.oss.simulacron.common.result.ConfigurationErrorResult;
import com.datastax.oss.simulacron.common.result.FunctionFailureResult;
import com.datastax.oss.simulacron.common.result.InvalidResult;
import com.datastax.oss.simulacron.common.result.IsBootstrappingResult;
import com.datastax.oss.simulacron.common.result.NoResult;
import com.datastax.oss.simulacron.common.result.OverloadedResult;
import com.datastax.oss.simulacron.common.result.ProtocolErrorResult;
import com.datastax.oss.simulacron.common.result.ReadFailureResult;
import com.datastax.oss.simulacron.common.result.ReadTimeoutResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.ServerErrorResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.result.TruncateErrorResult;
import com.datastax.oss.simulacron.common.result.UnauthorizedResult;
import com.datastax.oss.simulacron.common.result.UnavailableResult;
import com.datastax.oss.simulacron.common.result.UnpreparedResult;
import com.datastax.oss.simulacron.common.result.VoidResult;
import com.datastax.oss.simulacron.common.result.WriteFailureResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
 * </code>
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
      LinkedHashMap<String, Object> params,
      LinkedHashMap<String, String> paramTypes) {
    return new Query(query, consistencies, params, paramTypes);
  }

  /**
   * Provides a 'Query' request instance.
   *
   * @param query The query string.
   * @param consistencies The consistency to match on, or empty list to match on all.
   * @return a query instance.
   */
  public static Query query(String query, List<ConsistencyLevel> consistencies) {
    return query(query, consistencies, null, null);
  }

  /**
   * Provides a 'Query' request instance.
   *
   * @param query The query string.
   * @param consistency The consistency to match on, or empty list to match on all.
   * @return a query instance.
   */
  public static Query query(String query, ConsistencyLevel consistency) {
    return query(query, Collections.singletonList(consistency));
  }

  private static final LinkedHashMap<String, String> noRowsColumnTypes = new LinkedHashMap<>();

  static {
    noRowsColumnTypes.put("fake", "varchar");
  }

  private static final List<LinkedHashMap<String, Object>> emptyRows = new ArrayList<>();

  /**
   * Provides a Rows result with no rows.
   *
   * @return A rows response with no rows.
   */
  public static SuccessResult noRows() {
    return new SuccessResult(emptyRows, noRowsColumnTypes);
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
      List<LinkedHashMap<String, Object>> rows, LinkedHashMap<String, String> columnTypes) {
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
   * Provides an Already exists error response instance for a given keyspace.
   *
   * @param keyspace The keyspace for the error.
   * @return the generated error result.
   */
  public static AlreadyExistsResult alreadyExists(String keyspace) {
    return new AlreadyExistsResult("unused", keyspace);
  }

  /**
   * Provides an Already exists error response instance for a given table.
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
      DisconnectAction.Scope scope, CloseType closeType) {
    return new CloseConnectionResult(scope, closeType);
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
   * Provides a 'void' response.
   *
   * @return The generated void result.
   */
  public static VoidResult void_() {
    return new VoidResult();
  }

  /**
   * Provides an indication that no response should be sent for the given prime.
   *
   * @return The generated no result.
   */
  public static NoResult noResult() {
    return new NoResult();
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

    /**
     * Convenience for passing a row builder directly to then so one doesn't have to call build()
     * themselves.
     *
     * @param rowBuilder The rowBuilder to use build() to make a then.
     * @return this builder
     */
    public PrimeBuilder then(RowBuilder rowBuilder) {
      this.then = rowBuilder.build();
      return this;
    }

    /**
     * Adds a delay to the prime.
     *
     * @param delay How long to delay
     * @param delayUnit The unit of the delay
     * @return this builder
     */
    public PrimeBuilder delay(long delay, TimeUnit delayUnit) {
      if (then == null) {
        throw new RuntimeException("then must be called before delay.");
      }
      then.setDelay(delay, delayUnit);
      return this;
    }

    /**
     * Indicates that the prime should not apply to a matched prepare message. This is the default
     * behavior so this method doesn't need to be used unless you want your code to be more
     * explicit.
     *
     * @return this builder
     */
    public PrimeBuilder ignoreOnPrepare() {
      if (then == null) {
        throw new RuntimeException("then must be called before ignoreOnPrepare.");
      }
      then.setIgnoreOnPrepare(true);
      return this;
    }

    /**
     * Indicates that the prime should apply to a matched prepare message.
     *
     * @return this builder
     */
    public PrimeBuilder applyToPrepare() {
      if (then == null) {
        throw new RuntimeException("then must be called before applyToPrepare.");
      }
      then.setIgnoreOnPrepare(false);
      return this;
    }

    /** @return a {@link Prime} from this configuration. */
    public Prime build() {
      return new Prime(new RequestPrime(when, then));
    }
  }

  public static class RowBuilder {
    List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
    LinkedHashMap<String, String> columnTypes;

    public RowBuilder row(Object... data) {
      LinkedHashMap<String, Object> rowData = new LinkedHashMap<>();
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
      columnTypes = new LinkedHashMap<>();
      for (int i = 0; i < columns.length - 1; i += 2) {
        columnTypes.put(columns[i], columns[i + 1]);
      }

      return this;
    }

    public SuccessResult build() {
      // If no columnTypes are found, provide empty list which assumes varchar.
      if (columnTypes == null) {
        columnTypes = new LinkedHashMap<>();
      }
      return new SuccessResult(rows, columnTypes);
    }
  }
}
