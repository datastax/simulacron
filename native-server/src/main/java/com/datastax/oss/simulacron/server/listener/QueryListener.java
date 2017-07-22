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
package com.datastax.oss.simulacron.server.listener;

import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundNode;

@FunctionalInterface
/**
 * A listener that gets invoked whenever a query is received on a node. Note that this method blocks
 * the calling thread (event loop) that received the message so it is recommended not to do anything
 * intensive of blocking in the apply implementation.
 */
public interface QueryListener {
  void apply(BoundNode boundNode, QueryLog queryLog);
}
