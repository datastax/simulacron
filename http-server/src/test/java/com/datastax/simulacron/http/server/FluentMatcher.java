package com.datastax.simulacron.http.server;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;

import java.util.function.Predicate;

public class FluentMatcher {

  /**
   * Convenience method for wrapping a {@link Predicate} into a matcher.
   *
   * @param function the predicate to wrap.
   * @param <T> The expected input type.
   * @return a {@link CustomTypeSafeMatcher} that simply wraps the input predicate.
   */
  public static <T> Matcher<T> match(Predicate<T> function) {
    return new CustomTypeSafeMatcher<T>("Did not match") {

      @Override
      protected boolean matchesSafely(T item) {
        return function.test(item);
      }
    };
  }
}
