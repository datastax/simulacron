/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common.codec;

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bunch of static convenience wrappers, mostly to make constructing values around column and row
 * data more fluent.
 */
public class CodecUtils {

  public static Logger logger = LoggerFactory.getLogger(CodecUtils.class);

  static final Map<String, RawType> nativeTypeMap = new HashMap<>();

  static {
    // preload primitive types
    Field[] fields = ProtocolConstants.DataType.class.getDeclaredFields();

    // types to skip
    Set<String> nonPrimitiveTypes = new TreeSet<>();
    nonPrimitiveTypes.add("custom");
    nonPrimitiveTypes.add("list");
    nonPrimitiveTypes.add("map");
    nonPrimitiveTypes.add("set");
    nonPrimitiveTypes.add("udt");
    nonPrimitiveTypes.add("tuple");

    for (Field field : fields) {
      try {
        String fieldName = field.getName().toLowerCase();
        if (nonPrimitiveTypes.contains(fieldName) || field.isSynthetic()) {
          continue;
        }
        RawType rawType = primitive((int) field.get(null));
        nativeTypeMap.put(fieldName, rawType);
      } catch (IllegalAccessException e) {
        logger.error("Unable to cache Primitive type for {}", field, e);
      }
    }
  }

  private static final ConcurrentMap<String, RawType> typeCache = new ConcurrentHashMap<>();

  /**
   * Convenience wrapper for producing a row from a variable number of column values.
   *
   * @param columns column value {@link ByteBuffer}s to map to a Row.
   * @return a List of the input column buffers.
   */
  public static List<ByteBuffer> row(ByteBuffer... columns) {
    return new ArrayList<>(Arrays.asList(columns));
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
      AbstractNode node, Encoder<T> encoder, String key, T defaultValue) {
    // TODO: Handle type conversions
    return encoder.apply(node.resolvePeerInfo(key, defaultValue));
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
   * Resolves the {@link RawType} from the given name. If not found returns null.
   *
   * @param name name to look up, if null assumes varchar.
   * @return The associated {@link RawType} for this name.
   */
  public static RawType getTypeFromName(String name) {
    if (name == null) {
      name = "varchar";
    }
    RawType rawType = nativeTypeMap.get(name);
    if (rawType == null) {
      rawType = typeCache.computeIfAbsent(name, RawTypeParser::resolveRawTypeFromName);
    }
    return rawType;
  }

  /**
   * Convenience wrapper for producing a list of {@link ColumnSpec}s from a variable argument list.
   *
   * @param colSpecs Column specs to put in list.
   * @return List of input {@link ColumnSpec}s
   */
  public static List<ColumnSpec> columnSpecs(ColumnSpec... colSpecs) {
    return new ArrayList<>(Arrays.asList(colSpecs));
  }

  /**
   * @return A convenient version of {@link #columnSpecBuilder(String, String)} that prefills
   *     keyspace and table information which is not considered relevant in the response metadata.
   */
  public static ColumnSpecBuilder columnSpecBuilder() {
    return columnSpecBuilder("ks", "tbl");
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
    AtomicInteger counter = new AtomicInteger(0);
    return (columnName, rawType) ->
        new ColumnSpec(keyspace, table, columnName, counter.getAndIncrement(), rawType);
  }

  /** see {@link #columnSpecBuilder(String, String)} */
  public interface ColumnSpecBuilder extends BiFunction<String, RawType, ColumnSpec> {}
}
