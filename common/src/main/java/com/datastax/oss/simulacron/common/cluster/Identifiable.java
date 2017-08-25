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
package com.datastax.oss.simulacron.common.cluster;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import com.fasterxml.jackson.annotation.JsonInclude;

public interface Identifiable extends Comparable<Identifiable> {
  /** @return A unique id for this. */
  @JsonInclude(NON_NULL)
  Long getId();

  @Override
  default int compareTo(Identifiable other) {
    // by default compare by id, this is not perfect if comparing cross-dc or comparing different types but in the
    // general case this should only be used for comparing for things in same group.
    return (int) (this.getId() - other.getId());
  }
}
