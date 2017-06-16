package com.datastax.simulacron.common.cluster;

import com.datastax.simulacron.common.request.Query;
import com.datastax.simulacron.common.request.Request;
import com.datastax.simulacron.common.result.NoResult;
import com.datastax.simulacron.common.result.Result;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class RequestPrime {
  public final Request when;
  public final Result then;

  public RequestPrime(String query, Result then) {
    this(new Query(query), then);
  }

  @JsonCreator
  public RequestPrime(@JsonProperty("when") Request when, @JsonProperty("then") Result then) {
    this.when = when;
    if (then == null) {
      this.then = new NoResult();
    } else {
      this.then = then;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RequestPrime that = (RequestPrime) o;

    if (when != null ? !when.equals(that.when) : that.when != null) return false;
    return then != null ? then.equals(that.then) : that.then == null;
  }

  @Override
  public int hashCode() {
    int result = when != null ? when.hashCode() : 0;
    result = 31 * result + (then != null ? then.hashCode() : 0);
    return result;
  }
}
