package com.datastax.simulacron.common.codec;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface Encoder<T> extends Function<T, ByteBuffer> {}
