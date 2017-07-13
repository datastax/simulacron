package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.simulacron.common.cluster.AbstractNode;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public abstract class ErrorResult extends Result {

  @JsonProperty("message")
  protected final String errorMessage;

  @JsonIgnore private final transient int errorCode;

  ErrorResult(int errorCode, String errorMessage, long delayInMs) {
    super(delayInMs);
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  public int getErrorCode() {
    return this.errorCode;
  }

  @Override
  public List<Action> toActions(AbstractNode node, Frame frame) {
    return Collections.singletonList(new MessageResponseAction(toMessage(), getDelayInMs()));
  }

  public Message toMessage() {
    return new Error(getErrorCode(), getErrorMessage());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    ErrorResult that = (ErrorResult) o;

    if (errorCode != that.errorCode) return false;
    return errorMessage != null
        ? errorMessage.equals(that.errorMessage)
        : that.errorMessage == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (errorMessage != null ? errorMessage.hashCode() : 0);
    result = 31 * result + errorCode;
    return result;
  }
}
