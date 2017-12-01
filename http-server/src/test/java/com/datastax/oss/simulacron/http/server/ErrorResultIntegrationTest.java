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
package com.datastax.oss.simulacron.http.server;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.alreadyExists;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.authenticationError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.closeConnection;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.configurationError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.functionFailure;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.invalid;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.isBootstrapping;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noResult;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.overloaded;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.protocolError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readFailure;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readTimeout;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.syntaxError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.truncateError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unauthorized;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unprepared;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.void_;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.writeFailure;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.writeTimeout;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static com.datastax.oss.simulacron.http.server.FluentMatcher.match;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.instanceOf;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.BootstrappingException;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ProtocolError;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.TransportException;
import com.datastax.driver.core.exceptions.TruncateException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.UnpreparedException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.RequestFailureReason;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ErrorResultIntegrationTest {

  @Rule public AdminServer server = new AdminServer(ClusterSpec.builder().withNodes(1).build());

  @Rule public ExpectedException thrown = ExpectedException.none();

  private final String query = "select * from foo";

  @Test
  public void testShouldReturnVoid() throws Exception {
    // not technically an error, but this ensures void response work.
    server.prime(when(query).then(void_()));

    assertThat(query().getAvailableWithoutFetching()).isEqualTo(0);
  }

  @Test
  public void testShouldReturnServerError() throws Exception {
    String errMessage = "This is a server error";
    server.prime(when(query).then(serverError(errMessage)));

    thrown.expect(ServerError.class);
    thrown.expectMessage(endsWith(errMessage));
    query();
  }

  @Test
  public void testShouldReturnAlreadyExists() throws Exception {
    String keyspace = "ks";
    String table = "tbl";
    server.prime(when(query).then(alreadyExists(keyspace, table)));

    thrown.expect(AlreadyExistsException.class);
    thrown.expectMessage(containsString(keyspace + "." + table));
    query();
  }

  @Test
  public void testShouldReturnAuthenticationError() throws Exception {
    String message = "Invalid password, man!";
    server.prime(when(query).then(authenticationError(message)));

    thrown.expect(AuthenticationException.class);
    thrown.expectMessage(endsWith(message));
    query();
  }

  @Test
  public void testShouldReturnConfigurationError() throws Exception {
    String message = "This is a config error";
    server.prime(when(query).then(configurationError(message)));

    thrown.expect(InvalidConfigurationInQueryException.class);
    thrown.expectMessage(endsWith(message));
    query();
  }

  @Test
  public void testShouldReturnFunctionFailure() throws Exception {
    String detail = "function fail";
    String keyspace = "ks";
    String function = "fun";
    List<String> argTypes = new ArrayList<>();
    argTypes.add("text");
    argTypes.add("int");
    server.prime(when(query).then(functionFailure(keyspace, function, argTypes, detail)));

    thrown.expect(FunctionExecutionException.class);
    thrown.expectMessage(
        String.format(
            "execution of '%s.%s[%s]' failed: %s",
            keyspace, function, String.join(", ", argTypes), detail));
    query();
  }

  @Test
  public void testShouldReturnInvalidResult() throws Exception {
    String message = "This is an invalid result";
    server.prime(when(query).then(invalid(message)));

    thrown.expect(InvalidQueryException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnIsBootstrapping() throws Exception {
    server.prime(when(query).then(isBootstrapping()));

    thrown.expect(hasHostError(instanceOf(BootstrappingException.class)));
    query();
  }

  @Test
  public void testShouldReturnOverloaded() throws Exception {
    String message = "DANGER WILL ROBINSON!";
    server.prime(when(query).then(overloaded(message)));

    thrown.expect(OverloadedException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnProtocolError() throws Exception {
    String message = "Invalid Protocol Message";
    server.prime(when(query).then(protocolError(message)));

    thrown.expect(ProtocolError.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnSyntaxError() throws Exception {
    String message = "pc load letter";
    server.prime(when(query).then(syntaxError(message)));

    thrown.expect(SyntaxError.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnSyntaxErrorOnPrepare() throws Exception {
    String message = "this syntax is no good";
    server.prime(when(query).then(syntaxError(message)).applyToPrepare());

    thrown.expect(SyntaxError.class);
    thrown.expectMessage(message);

    prepare();
  }

  @Test
  public void testShouldNotReturnSyntaxErrorOnPrepare() throws Exception {
    String message = "this syntax is no good";
    server.prime(when(query).then(syntaxError(message)).ignoreOnPrepare());

    // should not throw error here.
    prepare();

    thrown.expect(SyntaxError.class);
    thrown.expectMessage(message);

    prepareAndQuery();
  }

  @Test
  public void testShouldNotReturnSyntaxErrorOnPrepareByDefault() throws Exception {
    String message = "this syntax is no good";
    server.prime(when(query).then(syntaxError(message)));

    // should not throw error here.
    prepare();

    thrown.expect(SyntaxError.class);
    thrown.expectMessage(message);

    prepareAndQuery();
  }

  @Test
  public void testShouldReturnTruncateError() throws Exception {
    String message = "This is a truncate error";
    server.prime(when(query).then(truncateError(message)));

    thrown.expect(TruncateException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnUnauthorized() throws Exception {
    String message = "unacceptable";
    server.prime(when(query).then(unauthorized(message)));

    thrown.expect(UnauthorizedException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  @Ignore("Skipping as this creates a loop in the driver")
  public void testShouldReturnUnprepared() throws Exception {
    String message = "I'm not ready for this";
    server.prime(when(query).then(unprepared(message)));

    thrown.expect(UnpreparedException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnReadFailure() throws Exception {
    Map<InetAddress, RequestFailureReason> reasons = new LinkedHashMap<>();
    InetAddress addr = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    reasons.put(addr, RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
    server.prime(when(query).then(readFailure(ConsistencyLevel.THREE, 1, 4, reasons, true)));

    thrown.expect(ReadFailureException.class);
    thrown.expect(
        match(
            (ReadFailureException rfe) ->
                rfe.getFailures() == 1
                    && rfe.getReceivedAcknowledgements() == 1
                    && rfe.getRequiredAcknowledgements() == 4
                    && rfe.wasDataRetrieved()
                    && rfe.getConsistencyLevel()
                        == com.datastax.driver.core.ConsistencyLevel.THREE));
    query();
  }

  @Test
  public void testShouldReturnWriteFailure() throws Exception {
    Map<InetAddress, RequestFailureReason> reasons = new LinkedHashMap<>();
    InetAddress addr = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    reasons.put(addr, RequestFailureReason.UNKNOWN);
    server.prime(
        when(query)
            .then(writeFailure(ConsistencyLevel.EACH_QUORUM, 1, 7, reasons, WriteType.SIMPLE)));

    thrown.expect(WriteFailureException.class);
    thrown.expect(
        match(
            (WriteFailureException wfe) ->
                wfe.getFailures() == 1
                    && wfe.getReceivedAcknowledgements() == 1
                    && wfe.getRequiredAcknowledgements() == 7
                    && wfe.getWriteType() == com.datastax.driver.core.WriteType.SIMPLE
                    && wfe.getConsistencyLevel()
                        == com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM));
    query();
  }

  @Test
  public void testShouldReturnUnavailable() throws Exception {
    server.prime(when(query).then(unavailable(ConsistencyLevel.ALL, 5, 1)));

    thrown.expect(UnavailableException.class);
    thrown.expect(
        match(
            (UnavailableException uae) ->
                uae.getAliveReplicas() == 1
                    && uae.getRequiredReplicas() == 5
                    && uae.getConsistencyLevel() == com.datastax.driver.core.ConsistencyLevel.ALL));
    query();
  }

  @Test
  public void testShouldReturnWriteTimeout() throws Exception {
    server.prime(when(query).then(writeTimeout(ConsistencyLevel.QUORUM, 3, 1, WriteType.BATCH)));

    thrown.expect(WriteTimeoutException.class);
    thrown.expect(
        match(
            (WriteTimeoutException wte) ->
                wte.getRequiredAcknowledgements() == 1
                    && wte.getReceivedAcknowledgements() == 3
                    && wte.getConsistencyLevel() == com.datastax.driver.core.ConsistencyLevel.QUORUM
                    && wte.getWriteType() == com.datastax.driver.core.WriteType.BATCH));

    query();
  }

  @Test
  public void testShouldReturnReadTimeout() throws Exception {
    server.prime(when(query).then(readTimeout(ConsistencyLevel.TWO, 1, 2, false)));

    thrown.expect(ReadTimeoutException.class);
    thrown.expect(
        match(
            (ReadTimeoutException rte) ->
                rte.getRequiredAcknowledgements() == 2
                    && rte.getReceivedAcknowledgements() == 1
                    && !rte.wasDataRetrieved()
                    && rte.getConsistencyLevel() == com.datastax.driver.core.ConsistencyLevel.TWO));
    query();
  }

  @Test
  public void testShouldReturnClientTimeout() throws Exception {
    server.prime(when(query));

    thrown.expect(OperationTimedOutException.class);
    query(new SimpleStatement(query).setReadTimeoutMillis(1000));
  }

  @Test
  public void testShouldReturnClientTimeoutWhenUsingNoResult() throws Exception {
    server.prime(when(query).then(noResult()));
    thrown.expect(OperationTimedOutException.class);
    query(new SimpleStatement(query).setReadTimeoutMillis(1000));
  }

  @Test
  public void testShouldReturnTransportException() throws Exception {
    server.prime(
        when(query).then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    // expect a transport exception as the connection should have been closed.
    thrown.expect(TransportException.class);
    query();
  }

  private ResultSet query() throws Exception {
    return query(new SimpleStatement(query));
  }

  private PreparedStatement prepare() throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      return driverCluster.connect().prepare(query);
    }
  }

  private ResultSet prepareAndQuery() throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      Session session = driverCluster.connect();
      PreparedStatement prepared = session.prepare(query);
      return session.execute(prepared.bind());
    }
  }

  private ResultSet query(Statement statement) throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      return driverCluster.connect().execute(statement);
    }
  }

  private NoHostAvailableMatcher hasHostError(Matcher<Exception> matcher) {
    return new NoHostAvailableMatcher(matcher);
  }

  private class NoHostAvailableMatcher extends BaseMatcher<Exception> {

    private final Matcher<Exception> expectedFirstErrorMatcher;

    NoHostAvailableMatcher(Matcher<Exception> expectedFirstErrorMatcher) {
      this.expectedFirstErrorMatcher = expectedFirstErrorMatcher;
    }

    @Override
    public boolean matches(Object item) {
      if (item instanceof NoHostAvailableException) {
        NoHostAvailableException nhae = (NoHostAvailableException) item;
        if (nhae.getErrors().size() == 1) {
          Throwable error = nhae.getErrors().values().iterator().next();
          return expectedFirstErrorMatcher.matches(error);
        }
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText(NoHostAvailableException.class + " with one host error conforming to ")
          .appendDescriptionOf(expectedFirstErrorMatcher);
    }
  }
}
