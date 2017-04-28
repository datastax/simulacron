package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.Error;

public class MessageBasedErrorResult extends ErrorResult {

  MessageBasedErrorResult(int errorCode, String errorMessage, long delayInMs) {
    super(errorCode, errorMessage, delayInMs);
  }

  @Override
  public Message toMessage() {
    return new Error(getErrorCode(), getErrorMessage());
  }
}
