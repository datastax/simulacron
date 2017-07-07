package com.datastax.simulacron.http.server;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.datastax.simulacron.common.codec.WriteType;
import com.datastax.simulacron.common.stubbing.CloseType;
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

import static com.datastax.simulacron.common.stubbing.PrimeDsl.*;
import static com.datastax.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static com.datastax.simulacron.http.server.FluentMatcher.match;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class ErrorResultIntegrationTest {

  @Rule public AdminServer server = new AdminServer(Cluster.builder().withNodes(1).build());

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
    server.prime(
        when(query)
            .then(writeFailure(ConsistencyLevel.EACH_QUORUM, 1, 7, reasons, WriteType.SIMPLE)));

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

  private ResultSet query(Statement statement) throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .allowBetaProtocolVersion()
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
