package com.datastax.simulacron.common.request;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.CqlMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

public final class Query extends Request {
  public final String query;
  public final transient List<ConsistencyLevel> consistencyEnum;
  public final Map<String, String> paramTypes;
  public final Map<String, Object> params;

  public Query(String query) {
    this(query, Collections.emptyList(), null, null);
  }

  @JsonCreator
  public Query(
      @JsonProperty("query") String query,
      @JsonProperty("consistency_level") String[] consistency,
      @JsonProperty("params") Map<String, Object> params,
      @JsonProperty("param_types") Map<String, String> paramTypes) {
    this(query, createEnumFromConsistency(consistency), params, paramTypes);
  }

  public Query(
      String query,
      List<ConsistencyLevel> consistencies,
      Map<String, Object> params,
      Map<String, String> paramTypes) {
    this.query = query;
    this.consistencyEnum = consistencies;
    this.params = params;
    this.paramTypes = paramTypes;
  }

  @JsonProperty("consistency_level")
  public String[] getConsistency() {
    String[] consistency = new String[consistencyEnum.size()];
    for (int i = 0; i < consistencyEnum.size(); i++) {
      consistency[i] = consistencyEnum.get(i).toString();
    }
    return consistency;
  }

  public boolean matches(Frame frame) {

    if (frame.message instanceof Prepare) {
      Prepare prepare = (Prepare) frame.message;
      if (query.equals(prepare.cqlQuery)) {
        return true;
      }
    }
    if (frame.message instanceof Execute) {
      Execute execute = (Execute) frame.message;
      Integer queryIdInt = new BigInteger(execute.queryId).intValue();
      if (getQueryId() == queryIdInt) {
        return checkParamsMatch(execute.options, frame);
      }
    }
    if (frame.message instanceof com.datastax.oss.protocol.internal.request.Query) {
      //If the query expected matches the primed query example and CL levels match. Return true;
      com.datastax.oss.protocol.internal.request.Query query =
          (com.datastax.oss.protocol.internal.request.Query) frame.message;
      if (this.query.equals(query.query)) {
        ConsistencyLevel level = ConsistencyLevel.fromCode(query.options.consistency);
        //NOTE: Absent CL level means it will match all CL levels
        if (this.consistencyEnum.contains(level) || this.consistencyEnum.size() == 0) {
          //Id any parameters are present we must make sure they match those in the primed queries
          return checkParamsMatch(query.options, frame);
        }
      }
    }
    return false;
  }

  /**
   * * A method that will check to see if primed query and actual query parameters match
   *
   * @param options query options to check.
   * @param frame The incoming frame.
   * @return True if the parameters match;
   */
  private boolean checkParamsMatch(QueryOptions options, Frame frame) {
    // I don't like this, but I can't see a way to simplify it.
    if (params == null) {
      if (options.namedValues.size() == 0 && options.positionalValues.size() == 0) return true;
      else return false;
    }
    if (options.namedValues.size() != params.size()
        && options.positionalValues.size() != params.size()) {
      return false;
    }
    if ((options.namedValues.size() != 0 || options.positionalValues.size() != 0)
        && params.size() != 0) {

      CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
      // Positional parameter case, ensure number of parameters is the same.
      if (options.positionalValues.size() != 0) {
        if (options.positionalValues.size() != params.size()) {
          return false;
        }
        Iterator<Object> primedPositionValues = params.values().iterator();
        Iterator<String> primedPositionTypes = paramTypes.values().iterator();
        // iterate over the parameters and make sure they all match
        for (ByteBuffer buffer : options.positionalValues) {
          if (!checkParamsEqual(
              buffer, primedPositionValues.next(), primedPositionTypes.next(), mapper)) {
            return false;
          }
        }
        // Named parameters case
      } else if (options.namedValues.size() != 0) {
        if (options.namedValues.size() != params.size()) {
          return false;
        }
        //look up each parameter and make sure they match
        for (String key : options.namedValues.keySet()) {
          String stringType = paramTypes.get(key);

          ByteBuffer buffer = options.namedValues.get(key);
          if (!checkParamsEqual(buffer, params.get(key), stringType, mapper)) {
            return false;
          }
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

  private static List<ConsistencyLevel> createEnumFromConsistency(String[] consistencies) {
    if (consistencies == null) {
      return new LinkedList<ConsistencyLevel>();
    }
    List<ConsistencyLevel> consistencyEnum = new LinkedList<ConsistencyLevel>();
    for (String consistency : consistencies) {
      consistencyEnum.add(ConsistencyLevel.fromString(consistency));
    }
    return consistencyEnum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Query query1 = (Query) o;

    if (query != null ? !query.equals(query1.query) : query1.query != null) return false;
    return consistencyEnum != null
        ? consistencyEnum.equals(query1.consistencyEnum)
        : query1.consistencyEnum == null;
  }

  @Override
  public int hashCode() {
    int result = query != null ? query.hashCode() : 0;
    result = 31 * result + (consistencyEnum != null ? consistencyEnum.hashCode() : 0);
    result = 31 * result + (paramTypes != null ? paramTypes.hashCode() : 0);
    result = 31 * result + (params != null ? params.hashCode() : 0);
    return result;
  }

  /**
   * Convience method to retrieve the queryId which happesn to be the hashcode associated query
   * string
   *
   * @return
   */
  @JsonIgnore
  public int getQueryId() {
    return query.hashCode();
  }
}
