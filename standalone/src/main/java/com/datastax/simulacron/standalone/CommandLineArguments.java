package com.datastax.simulacron.standalone;

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
