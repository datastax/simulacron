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
package com.datastax.oss.simulacron.standalone;

import ch.qos.logback.classic.Level;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

class CommandLineArguments {

  @Parameter(
    names = {"--ip", "-i"},
    description = "Starting IP address to create simulated C* nodes on"
  )
  String ipAddress = "127.0.0.1";

  @Parameter(
    names = {"--starting-port", "-s"},
    description =
        "Starting Port to assign Nodes to.  Note that if this is used multiple nodes can be assigned on one IP (which mimics C* 4.0+ peering)"
  )
  int startingPort = -1;

  @Parameter(
    names = {"--httpport", "-p"},
    description = "HTTP port to bind on"
  )
  int httpPort = 8187;

  @Parameter(
    names = {"--httpintf", "-t"},
    description = "Interface address to bind HTTP server on"
  )
  String httpInterface = "localhost";

  @Parameter(
    names = {"--verbose", "-v"},
    description = "Whether or not to enable verbose http logging (also enables DEBUG logging)"
  )
  boolean verbose = false;

  @Parameter(
    names = {"--loglevel", "-l"},
    description = "Logging level to use",
    converter = LevelConverter.class
  )
  Level logLevel = Level.INFO;

  @Parameter(
    names = {"--disable-activity-log", "-d"},
    description = "Disables activity logging by default"
  )
  boolean disableActivityLogging = false;

  @Parameter(
    names = {"--help", "-h"},
    hidden = true
  )
  boolean help = false;

  public static class LevelConverter implements IStringConverter<Level> {

    @Override
    public Level convert(String value) {
      Level convertedValue = Level.valueOf(value);

      if (convertedValue == null) {
        throw new ParameterException("Value " + value + "can not be converted to a logging level.");
      }
      return convertedValue;
    }
  }
}
