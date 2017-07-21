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
package com.datastax.oss.simulacron.common.cluster;

import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.request.Request;
import com.datastax.oss.simulacron.common.result.NoResult;
import com.datastax.oss.simulacron.common.result.Result;
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
