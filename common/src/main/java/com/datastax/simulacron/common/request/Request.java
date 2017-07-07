package com.datastax.simulacron.common.request;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.CqlMapper;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "request",
  defaultImpl = Query.class
)
@JsonSubTypes({
  @JsonSubTypes.Type(value = Query.class, name = "query"),
  @JsonSubTypes.Type(value = Options.class, name = "options"),
  @JsonSubTypes.Type(value = Batch.class, name = "batch"),
})
public abstract class Request {

  public abstract boolean matches(Frame frame);

  public static List<ConsistencyLevel> createEnumFromConsistency(String[] consistencies) {
    if (consistencies == null) {
      return new LinkedList<ConsistencyLevel>();
    }
    List<ConsistencyLevel> consistencyEnum = new LinkedList<ConsistencyLevel>();
    for (String consistency : consistencies) {
      consistencyEnum.add(ConsistencyLevel.fromString(consistency));
    }
    return consistencyEnum;
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
  public static boolean checkParamsEqual(
      ByteBuffer buffer, Object primedParam, String stringType, CqlMapper mapper) {
    if (primedParam.equals("*")) return true;
    RawType type = CodecUtils.getTypeFromName(stringType);
    Object nativeParamToCheck = mapper.codecFor(type).decode(buffer);
    Object primedParamToCheck = mapper.codecFor(type).toNativeType(primedParam);

    return primedParamToCheck.equals(nativeParamToCheck);
  }
}
