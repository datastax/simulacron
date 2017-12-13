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
package com.datastax.oss.simulacron.protocol.json;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Supported;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class NativeProtocolModule {

  private static final SimpleModule module = new SimpleModule("NativeProtocol");

  static {
    // Frame serializer
    module.addSerializer(Frame.class, new FrameSerializer());

    // Message serializers
    // Currently serializers are only implemented for requests as thats all we are interested in
    // serializing at the moment in Simulacron.
    module.addSerializer(Startup.class, new StartupSerializer());
    module.addSerializer(Authenticate.class, new AuthenticateSerializer());
    module.addSerializer(Supported.class, new SupportedSerializer());
    module.addSerializer(Query.class, new QuerySerializer());
    module.addSerializer(Prepare.class, new PrepareSerializer());
    module.addSerializer(Execute.class, new ExecuteSerializer());
    module.addSerializer(Register.class, new RegisterSerializer());
    module.addSerializer(Batch.class, new BatchSerializer());
    module.addSerializer(AuthResponse.class, new AuthResponseSerializer());
    // catch all for messages that are not implemented / needed (i.e. OPTIONS, READY).
    module.addSerializer(Message.class, new FallthroughMessageSerializer());

    // Query Options
    module.addSerializer(QueryOptions.class, new QueryOptionsSerializer());
  }

  public static SimpleModule module() {
    return module;
  }
}
