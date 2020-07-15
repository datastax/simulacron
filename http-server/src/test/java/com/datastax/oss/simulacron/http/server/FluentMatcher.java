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
package com.datastax.oss.simulacron.http.server;

import java.util.function.Predicate;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;

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
