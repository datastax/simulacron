package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.ALREADY_EXISTS;

public class AlreadyExistsResult extends ErrorResult {

  @JsonProperty("keyspace")
  private final String keyspace;

  @JsonProperty("table")
  private final String table;

  public AlreadyExistsResult(String errorMessage, String keyspace, String table) {
    this(errorMessage, keyspace, table, 0);
  }

  @JsonCreator
  public AlreadyExistsResult(
      @JsonProperty("message") String errorMessage,
      @JsonProperty(value = "keyspace", required = true) String keyspace,
      @JsonProperty(value = "table", required = true) String table,
      @JsonProperty("delayInMs") long delayInMs) {
    super(ALREADY_EXISTS, errorMessage, delayInMs);
    this.keyspace = keyspace;
    this.table = table;
  }

  @Override
  public Message toMessage() {
    return new AlreadyExists(errorMessage, keyspace, table);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    AlreadyExistsResult that = (AlreadyExistsResult) o;

    if (!keyspace.equals(that.keyspace)) return false;
    return table.equals(that.table);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + keyspace.hashCode();
    result = 31 * result + table.hashCode();
    return result;
  }
}
