package com.datastax.simulacron.http.server;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.datastax.simulacron.common.codec.WriteType;
import com.datastax.simulacron.common.result.*;
import com.datastax.simulacron.common.stubbing.DisconnectAction;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.simulacron.test.FluentMatcher.match;
import static com.datastax.simulacron.test.IntegrationUtils.defaultBuilder;
import static org.hamcrest.CoreMatchers.*;

public class ErrorResultIntegrationTest {

  @Rule public AdminServer server = new AdminServer(Cluster.builder().withNodes(1).build());

  @Rule public ExpectedException thrown = ExpectedException.none();

  private final String query = "select * from foo";

  @Test
  public void testShouldReturnServerError() throws Exception {
    String errMessage = "This is a server error";
    server.prime(new QueryPrime(query, new ServerErrorResult(errMessage)));

    thrown.expect(ServerError.class);
    thrown.expectMessage(endsWith(errMessage));
    query();
  }

  @Test
  public void testShouldReturnAlreadyExists() throws Exception {
    String keyspace = "ks";
    String table = "tbl";
    server.prime(new QueryPrime(query, new AlreadyExistsResult("unused", keyspace, table)));

    thrown.expect(AlreadyExistsException.class);
    thrown.expectMessage(containsString(keyspace + "." + table));
    query();
  }

  @Test
  public void testShouldReturnAuthenticationError() throws Exception {
    String message = "Invalid password, man!";
    server.prime(new QueryPrime(query, new AuthenticationErrorResult(message)));

    thrown.expect(AuthenticationException.class);
    thrown.expectMessage(endsWith(message));
    query();
  }

  @Test
  public void testShouldReturnConfigurationError() throws Exception {
    String message = "This is a config error";
    server.prime(new QueryPrime(query, new ConfigurationErrorResult(message)));

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
    server.prime(
        new QueryPrime(query, new FunctionFailureResult(keyspace, function, argTypes, detail)));

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
    server.prime(new QueryPrime(query, new InvalidResult(message)));

    thrown.expect(InvalidQueryException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnIsBootstrapping() throws Exception {
    server.prime(new QueryPrime(query, new IsBoostrappingResult()));

    thrown.expect(hasHostError(instanceOf(BootstrappingException.class)));
    query();
  }

  @Test
  public void testShouldReturnOverloaded() throws Exception {
    String message = "DANGER WILL ROBINSON!";
    server.prime(new QueryPrime(query, new OverloadedResult(message)));

    thrown.expect(OverloadedException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnProtocolError() throws Exception {
    String message = "Invalid Protocol Message";
    server.prime(new QueryPrime(query, new ProtocolErrorResult(message)));

    thrown.expect(ProtocolError.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnSyntaxError() throws Exception {
    String message = "pc load letter";
    server.prime(new QueryPrime(query, new SyntaxErrorResult(message)));

    thrown.expect(SyntaxError.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnTruncateError() throws Exception {
    String message = "This is a truncate error";
    server.prime(new QueryPrime(query, new TruncateErrorResult(message)));

    thrown.expect(TruncateException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnUnauthorized() throws Exception {
    String message = "unacceptable";
    server.prime(new QueryPrime(query, new UnauthorizedResult(message)));

    thrown.expect(UnauthorizedException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  @Ignore("Skipping as this creates a loop in the driver")
  public void testShouldReturnUnprepared() throws Exception {
    String message = "I'm not ready for this";
    server.prime(new QueryPrime(query, new UnpreparedResult(message)));

    thrown.expect(UnpreparedException.class);
    thrown.expectMessage(message);
    query();
  }

  @Test
  public void testShouldReturnReadFailure() throws Exception {
    Map<InetAddress, RequestFailureReason> reasons = new LinkedHashMap<>();
    InetAddress addr = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    reasons.put(addr, RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
    ReadFailureResult result = new ReadFailureResult(ConsistencyLevel.THREE, 1, 4, reasons, true);
    server.prime(new QueryPrime(query, result));

    thrown.expect(ReadFailureException.class);
    thrown.expect(
        match(
            (ReadFailureException rfe) ->
                rfe.getFailures() == 1
                    && rfe.getFailuresMap().get(addr)
                        == RequestFailureReason.READ_TOO_MANY_TOMBSTONES.getCode()
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
    WriteFailureResult result =
        new WriteFailureResult(ConsistencyLevel.EACH_QUORUM, 1, 7, reasons, WriteType.SIMPLE);
    server.prime(new QueryPrime(query, result));

    thrown.expect(WriteFailureException.class);
    thrown.expect(
        match(
            (WriteFailureException wfe) ->
                wfe.getFailures() == 1
                    && wfe.getFailuresMap().get(addr) == RequestFailureReason.UNKNOWN.getCode()
                    && wfe.getReceivedAcknowledgements() == 1
                    && wfe.getRequiredAcknowledgements() == 7
                    && wfe.getWriteType() == com.datastax.driver.core.WriteType.SIMPLE
                    && wfe.getConsistencyLevel()
                        == com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM));
    query();
  }

  @Test
  public void testShouldReturnUnavailable() throws Exception {
    UnavailableResult result = new UnavailableResult(ConsistencyLevel.ALL, 5, 1);
    server.prime(new QueryPrime(query, result));

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
    WriteTimeoutResult result =
        new WriteTimeoutResult(ConsistencyLevel.QUORUM, 3, 1, WriteType.BATCH);
    server.prime(new QueryPrime(query, result));

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
    ReadTimeoutResult result = new ReadTimeoutResult(ConsistencyLevel.TWO, 1, 2, false);
    server.prime(new QueryPrime(query, result));

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
    server.prime(new QueryPrime(query, new NoResult()));

    thrown.expect(OperationTimedOutException.class);
    query(new SimpleStatement(query).setReadTimeoutMillis(1000));
  }

  @Test
  public void testShouldReturnTransportException() throws Exception {
    CloseConnectionResult result =
        new CloseConnectionResult(
            DisconnectAction.Scope.CONNECTION, DisconnectAction.CloseType.DISCONNECT, 0);
    server.prime(new QueryPrime(query, result));

    // expect a transport exception as the connection should have been closed.
    thrown.expect(TransportException.class);
    query();
  }

  private void query() throws Exception {
    query(new SimpleStatement(query));
  }

  private void query(Statement statement) throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .allowBetaProtocolVersion()
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      driverCluster.connect().execute(statement);
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
