/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import static com.datastax.oss.simulacron.common.codec.CodecUtils.nativeTypeMap;

import com.datastax.oss.protocol.internal.response.result.RawType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A parser for parsing a type name as a {@link String} to a {@link RawType}, i.e. 'ascii' -> new
 * RawType(ascii).
 *
 * <p>This is largely adapted from DataTypeCqlNameParser from the DataStax Java Driver for Apache
 * Cassandra. See NOTICE.txt for more details.
 */
class RawTypeParser {

  public static Logger logger = LoggerFactory.getLogger(RawTypeParser.class);

  private final String str;

  private int idx;

  private RawTypeParser(String str, int idx) {
    this.str = str;
    this.idx = idx;
  }

  static RawType resolveRawTypeFromName(String name) {
    if (name.startsWith("'")) {
      return new RawType.RawCustom(name.substring(1, name.length() - 1));
    }

    RawTypeParser parser = new RawTypeParser(name, 0);
    String type = parser.parseTypeName();

    RawType nativeType = nativeTypeMap.get(type.toLowerCase());
    if (nativeType != null) {
      return nativeType;
    }

    if (type.equalsIgnoreCase("list")) {
      List<String> parameters = parser.parseTypeParameters();
      if (parameters.size() != 1) {
        throw new InvalidTypeException(
            String.format("Excepting single parameter for list, got %s", parameters));
      }
      RawType elementType = resolveRawTypeFromName(parameters.get(0));
      return new RawType.RawList(elementType);
    }

    if (type.equalsIgnoreCase("set")) {
      List<String> parameters = parser.parseTypeParameters();
      if (parameters.size() != 1) {
        throw new InvalidTypeException(
            String.format("Excepting single parameter for set, got %s", parameters));
      }
      RawType elementType = resolveRawTypeFromName(parameters.get(0));
      return new RawType.RawSet(elementType);
    }

    if (type.equalsIgnoreCase("map")) {
      List<String> parameters = parser.parseTypeParameters();
      if (parameters.size() != 2) {
        throw new InvalidTypeException(
            String.format("Expecting two parameters for map, got %s", parameters));
      }
      RawType keyType = resolveRawTypeFromName(parameters.get(0));
      RawType valueType = resolveRawTypeFromName(parameters.get(1));
      return new RawType.RawMap(keyType, valueType);
    }

    if (type.equalsIgnoreCase("tuple")) {
      List<String> rawTypes = parser.parseTypeParameters();
      if (rawTypes.size() == 0) {
        throw new InvalidTypeException(
            String.format("Expecting at least one parameter for tuple, got %s", rawTypes));
      }
      List<RawType> types = new ArrayList<>(rawTypes.size());
      for (String rawType : rawTypes) {
        types.add(resolveRawTypeFromName(rawType));
      }
      return new RawType.RawTuple(types);
    }

    if (type.equalsIgnoreCase("empty")) {
      return new RawType.RawCustom(type);
    }

    throw new InvalidTypeException("No valid type found");
  }

  private static int skipSpaces(String toParse, int idx) {
    while (idx < toParse.length() && isBlank(toParse.charAt(idx))) ++idx;
    return idx;
  }

  private static boolean isBlank(int c) {
    return c == ' ' || c == '\t' || c == '\n';
  }

  private static boolean isIdentifierChar(int c) {
    return (c >= '0' && c <= '9')
        || (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z')
        || c == '-'
        || c == '+'
        || c == '.'
        || c == '_'
        || c == '&';
  }

  private String parseTypeName() {
    idx = skipSpaces(str, idx);
    return readNextIdentifier();
  }

  private List<String> parseTypeParameters() {
    List<String> list = new ArrayList<String>();

    if (isEOS()) return list;

    skipBlankAndComma();

    if (str.charAt(idx) != '<') throw new InvalidTypeException("Illegal character " + idx);

    ++idx; // skipping '<'

    while (skipBlankAndComma()) {
      if (str.charAt(idx) == '>') {
        ++idx;
        return list;
      }

      String name = parseTypeName();
      String args = readRawTypeParameters();
      list.add(name + args);
    }
    throw new InvalidTypeException(
        String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
  }

  // left idx positioned on the character stopping the read
  private String readNextIdentifier() {
    int startIdx = idx;
    if (startIdx >= str.length()) {
      throw new InvalidTypeException("No more identifiers to read at index " + idx);
    }
    if (str.charAt(startIdx) == '"') { // case-sensitive name included in double quotes
      ++idx;
      // read until closing quote.
      while (!isEOS()) {
        boolean atQuote = str.charAt(idx) == '"';
        ++idx;
        if (atQuote) {
          // if the next character is also a quote, this is an escaped
          // quote, continue reading, otherwise stop.
          if (!isEOS() && str.charAt(idx) == '"') ++idx;
          else break;
        }
      }
    } else if (str.charAt(startIdx) == '\'') { // custom type name included in single quotes
      ++idx;
      // read until closing quote.
      while (!isEOS() && str.charAt(idx++) != '\'') {
        /* loop */
      }
    } else {
      while (!isEOS() && (isIdentifierChar(str.charAt(idx)) || str.charAt(idx) == '"')) ++idx;
    }
    return str.substring(startIdx, idx);
  }

  // Assumes we have just read a type name and read it's potential arguments
  // blindly. I.e. it assume that either parsing is done or that we're on a '<'
  // and this reads everything up until the corresponding closing '>'. It
  // returns everything read, including the enclosing brackets.
  private String readRawTypeParameters() {
    idx = skipSpaces(str, idx);

    if (isEOS() || str.charAt(idx) == '>' || str.charAt(idx) == ',') return "";

    if (str.charAt(idx) != '<')
      throw new InvalidTypeException(
          String.format(
              "Expecting char %d of %s to be '<' but '%c' found", idx, str, str.charAt(idx)));

    int i = idx;
    int open = 1;
    boolean inQuotes = false;
    while (open > 0) {
      ++idx;

      if (isEOS()) throw new InvalidTypeException("Non closed angle brackets");

      // Only parse for '<' and '>' characters if not within a quoted identifier.
      // Note we don't need to handle escaped quotes ("") in type names here, because they just cause inQuotes to flip
      // to false and immediately back to true
      if (!inQuotes) {
        if (str.charAt(idx) == '"') {
          inQuotes = true;
        } else if (str.charAt(idx) == '<') {
          open++;
        } else if (str.charAt(idx) == '>') {
          open--;
        }
      } else if (str.charAt(idx) == '"') {
        inQuotes = false;
      }
    }
    // we've stopped at the last closing ')' so move past that
    ++idx;
    return str.substring(i, idx);
  }

  // skip all blank and at best one comma, return true if there not EOS
  private boolean skipBlankAndComma() {
    boolean commaFound = false;
    while (!isEOS()) {
      int c = str.charAt(idx);
      if (c == ',') {
        if (commaFound) return true;
        else commaFound = true;
      } else if (!isBlank(c)) {
        return true;
      }
      ++idx;
    }
    return false;
  }

  private boolean isEOS() {
    return idx >= str.length();
  }
}
