# Contributing Guidelines

If you would like to contribute to the simulacron project, we would appreciate it!  Please be aware of the
following guidelines.

## Code formatting

We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See
https://github.com/google/google-java-format for IDE plugins. The rules are not configurable.

The build will fail if the code is not formatted. To format all files from the command line, run:

```
mvn fmt:format -Dformat.validateOnly=false
```

Some aspects are not covered by the formatter:
* imports: please configure your IDE to follow the guide (no wildcard imports, normal imports
  in ASCII sort order come first, followed by a blank line, followed by static imports in ASCII
  sort order).
* XML files: indent with two spaces and try to respect the column limit of 100 characters.

## GitHub Issues & Pull Requests

This project uses GitHub issues to track requests and ongoing work.  If you have a request, simply
create a GitHub issue.  If you would like to add a feature, create an issue first, or if the issue
already exists, either assign the issue to yourself or add a comment.

For any change you make, please push your branch to this repository and create a pull request.
We will try to review it quickly.

There is an expectation that tests are added for any new functionality or if otherwise applicable.
