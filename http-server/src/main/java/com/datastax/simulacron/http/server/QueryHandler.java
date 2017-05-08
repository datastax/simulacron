package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.CqlMapper;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.nio.ByteBuffer;
import java.util.Iterator;
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
          //Id any parameters are present we must make sure they match those in the primed queries
          if ((query.options.namedValues.size() != 0 || query.options.positionalValues.size() != 0)
              && primedQuery.when.params.size() != 0) {
            return checkParamsMatch(query, frame);
          }
          return true;
        }
      }
    }
    return false;
  }

  /**
   * * A method that will check to see if primed query and actual query parameters match
   *
   * @param query The query to check.
   * @param frame The incoming frame.
   * @return True if the parameters match;
   */
  private boolean checkParamsMatch(Query query, Frame frame) {
    CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
    // Positional parameter cause, ensure number of parameters is the same.
    if (query.options.positionalValues.size() != 0) {
      if (query.options.positionalValues.size() != primedQuery.when.params.size()) {
        return false;
      }
      Iterator<Object> primedPositionValues = primedQuery.when.params.values().iterator();
      Iterator<String> primedPositiontypes = primedQuery.when.paramTypes.values().iterator();
      // iterate over the parameters and make sure they all match
      for (ByteBuffer buffer : query.options.positionalValues) {
        if (!checkParamsEqual(
            buffer, primedPositionValues.next(), primedPositiontypes.next(), mapper)) {
          return false;
        }
      }
      // Named parameters case
    } else if (query.options.namedValues.size() != 0) {
      if (query.options.namedValues.size() != primedQuery.when.params.size()) {
        return false;
      }
      //look up each parameter and make sure they match
      for (String key : query.options.namedValues.keySet()) {
        String stringType = primedQuery.when.paramTypes.get(key);

        ByteBuffer buffer = query.options.namedValues.get(key);
        if (!checkParamsEqual(buffer, primedQuery.when.params.get(key), stringType, mapper)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * * Convience method to centralize parameter encoding decoding and equality checking
   *
   * @param buffer The buffer of the incoming query parameter
   * @param primedParam The corresponding primed parameter
   * @param stringType The parameter type
   * @param mapper Used for encoding decoding
   * @return True if they match otherwise false
   */
  private boolean checkParamsEqual(
      ByteBuffer buffer, Object primedParam, String stringType, CqlMapper mapper) {
    RawType type = CodecUtils.getTypeFromName(stringType);
    Object nativeParamToCheck = mapper.codecFor(type).decode(buffer);
    Object primedParamToCheck = mapper.codecFor(type).toNativeType(primedParam);
    if (primedParamToCheck.equals(nativeParamToCheck)) {
      return true;
    }
    return false;
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    return primedQuery.then.toActions(node, frame);
  }
}
