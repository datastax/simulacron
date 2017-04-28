package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.util.List;

public class QueryHandler implements StubMapping {
  QueryPrime primedQuery;

  public QueryHandler(QueryPrime primedQuery) {
    this.primedQuery = primedQuery;
  }

  @Override
  public boolean matches(Node node, Frame frame) {

    if (frame.message instanceof Query) {
      //If the query expected matches the primed query example and CL levels match. Return true;
      Query query = (Query) frame.message;
      if (primedQuery.when.query.equals(query.query)) {

        ConsistencyLevel level = ConsistencyLevel.fromCode(query.options.consistency);
        //NOTE: Absent CL level means it will match all CL levels
        if (primedQuery.when.consistencyEnum.contains(level)
            || primedQuery.when.consistencyEnum.size() == 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    return primedQuery.then.toActions(node, frame);
  }
}
