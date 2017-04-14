package com.datastax.simulacron.common.codec;

public class InvalidTypeException extends RuntimeException {

  public InvalidTypeException(String msg) {
    super(msg);
  }

  public InvalidTypeException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
