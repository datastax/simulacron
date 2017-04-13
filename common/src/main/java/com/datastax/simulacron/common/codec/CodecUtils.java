package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.Node;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.function.BiFunction;

/**
 * A bunch of static convenience wrappers, mostly to make constructing values around column and row
 * data more fluent.
 */
public class CodecUtils {

  /**
   * Convenience wrapper for producing a row from a variable number of column values.
   *
   * @param columns column value {@link ByteBuffer}s to map to a Row.
   * @return a List of the input column buffers.
   */
  public static List<ByteBuffer> row(ByteBuffer... columns) {
    return Arrays.asList(columns);
  }

  /**
   * Convenience wrapper for producing a data queue that would be used as input for {@link
   * com.datastax.oss.protocol.internal.response.result.Rows#Rows(RowsMetadata, Queue)} for a single
   * row, which is provided as an input of column buffers.
   *
   * @param columns column value {@link ByteBuffer}s to map to a single Row.
   * @return a {@link Queue} with a single list containing the input columns.
   */
  public static Queue<List<ByteBuffer>> singletonRow(ByteBuffer... columns) {
    List<ByteBuffer> row = row(columns);
    return rows(row);
  }

  /**
   * Convenience wrapper for producing a data queue that would be used as input for {@link
   * com.datastax.oss.protocol.internal.response.result.Rows#Rows(RowsMetadata, Queue)} for variable
   * number of list of columns (each representing a row).
   *
   * @param rows variable arguments of lists of rows.
   * @return a {@link Queue} with lists each representing a row.
   */
  @SafeVarargs
  public static Queue<List<ByteBuffer>> rows(List<ByteBuffer>... rows) {
    return new ArrayDeque<>(Arrays.asList(rows));
  }

  /**
   * Convenience wrapper for encoding peer info into a column bytebuffer using the given encoder.
   *
   * @param node Node to resolve peer info for.
   * @param encoder Encoder to use to encode peer info.
   * @param key key name to pull data from peer info.
   * @param defaultValue The default value if peer info is not previously set.
   * @param <T> The type of the value being encoded.
   * @return Encoded column buffer
   */
  @SuppressWarnings("unchecked")
  public static <T> ByteBuffer encodePeerInfo(
      Node node, Encoder<T> encoder, String key, T defaultValue) {
    // TODO: Handle type conversions
    return encoder.apply((T) node.resolvePeerInfo(key, defaultValue));
  }

  /**
   * Convenience wrapper for deriving a primitive type based on its type code.
   *
   * @param code Type code as defined in native protocol for the data type.
   * @return The resolved {@link RawType}.
   */
  public static RawType primitive(int code) {
    return RawType.PRIMITIVES.get(code);
  }

  /**
   * Convenience wrapper for producing a list of {@link ColumnSpec}s from a variable argument list.
   *
   * @param colSpecs Column specs to put in list.
   * @return List of input {@link ColumnSpec}s
   */
  public static List<ColumnSpec> columnSpecs(ColumnSpec... colSpecs) {
    return Arrays.asList(colSpecs);
  }

  /**
   * Convenience wrapper for building a function that creates {@link ColumnSpec} with a given
   * keyspace and table.
   *
   * @param keyspace The keyspace to use for any {@link ColumnSpec} created from this builder
   *     function.
   * @param table The table to use for any {@link ColumnSpec} created from this builder function.
   * @return A function that takes a column name and type and produces a {@link ColumnSpec}.
   */
  public static ColumnSpecBuilder columnSpecBuilder(String keyspace, String table) {
    return (columnName, rawType) -> new ColumnSpec(keyspace, table, columnName, rawType);
  }

  /** see {@link #columnSpecBuilder(String, String)} */
  public interface ColumnSpecBuilder extends BiFunction<String, RawType, ColumnSpec> {}
}
