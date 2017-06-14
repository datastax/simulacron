package com.datastax.simulacron.common.request;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.CqlMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.util.*;

public final class Query extends Request {
  public final String query;
  public final String[] consistency;
  public final List<ConsistencyLevel> consistencyEnum;
  public final Map<String, String> paramTypes;
  public final Map<String, Object> params;

  @JsonCreator
  public Query(
      @JsonProperty("query") String query,
      @JsonProperty("consistency") String[] consistency,
      @JsonProperty("params") Map<String, Object> params,
      @JsonProperty("paramTypes") Map<String, String> paramTypes) {
    this.query = query;
    this.consistency = consistency;
    this.consistencyEnum = createEnumFromConsistency(consistency);
    this.paramTypes = paramTypes;
    this.params = params;
  }

  public boolean matches(Frame frame) {

    if (frame.message instanceof com.datastax.oss.protocol.internal.request.Query) {
      //If the query expected matches the primed query example and CL levels match. Return true;
      com.datastax.oss.protocol.internal.request.Query query =
          (com.datastax.oss.protocol.internal.request.Query) frame.message;
      if (this.query.equals(query.query)) {
        ConsistencyLevel level = ConsistencyLevel.fromCode(query.options.consistency);
        //NOTE: Absent CL level means it will match all CL levels
        if (this.consistencyEnum.contains(level) || this.consistencyEnum.size() == 0) {
          //Id any parameters are present we must make sure they match those in the primed queries
          if ((query.options.namedValues.size() != 0 || query.options.positionalValues.size() != 0)
              && this.params.size() != 0) {
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
  private boolean checkParamsMatch(
      com.datastax.oss.protocol.internal.request.Query query, Frame frame) {
    CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
    // Positional parameter cause, ensure number of parameters is the same.
    if (query.options.positionalValues.size() != 0) {
      if (query.options.positionalValues.size() != this.params.size()) {
        return false;
      }
      Iterator<Object> primedPositionValues = this.params.values().iterator();
      Iterator<String> primedPositiontypes = this.paramTypes.values().iterator();
      // iterate over the parameters and make sure they all match
      for (ByteBuffer buffer : query.options.positionalValues) {
        if (!checkParamsEqual(
            buffer, primedPositionValues.next(), primedPositiontypes.next(), mapper)) {
          return false;
        }
      }
      // Named parameters case
    } else if (query.options.namedValues.size() != 0) {
      if (query.options.namedValues.size() != this.params.size()) {
        return false;
      }
      //look up each parameter and make sure they match
      for (String key : query.options.namedValues.keySet()) {
        String stringType = this.paramTypes.get(key);

        ByteBuffer buffer = query.options.namedValues.get(key);
        if (!checkParamsEqual(buffer, this.params.get(key), stringType, mapper)) {
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Query when = (Query) o;

    if (query != null ? !query.equals(when.query) : when.query != null) return false;
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(consistency, when.consistency)) return false;
    return consistencyEnum != null
        ? consistencyEnum.equals(when.consistencyEnum)
        : when.consistencyEnum == null;
  }

  @Override
  public int hashCode() {
    int result = query != null ? query.hashCode() : 0;
    result = 31 * result + Arrays.hashCode(consistency);
    result = 31 * result + (consistencyEnum != null ? consistencyEnum.hashCode() : 0);
    return result;
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
}
