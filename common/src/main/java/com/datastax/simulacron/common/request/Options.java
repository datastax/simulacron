package com.datastax.simulacron.common.request;

import com.datastax.oss.protocol.internal.Frame;

public class Options extends Request {
  public static Options INSTANCE = new Options();

  @Override
  public boolean matches(Frame frame) {
    if (frame.message instanceof com.datastax.oss.protocol.internal.request.Options) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Options;
  }
}
