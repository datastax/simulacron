package com.datastax.simulacron.common.request;

import com.datastax.oss.protocol.internal.Frame;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "request",
  defaultImpl = Query.class
)
@JsonSubTypes({
  @JsonSubTypes.Type(value = Query.class, name = "query"),
  @JsonSubTypes.Type(value = Options.class, name = "options"),
})
public abstract class Request {

  public abstract boolean matches(Frame frame);
}
