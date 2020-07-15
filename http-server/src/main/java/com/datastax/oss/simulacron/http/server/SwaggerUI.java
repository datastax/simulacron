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

import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

public class SwaggerUI implements HttpListener {

  @Override
  public void registerWithRouter(Router router) {
    StaticHandler staticHandler = StaticHandler.create();
    staticHandler.setWebRoot("META-INF/resources/webjars");
    router.route("/static/*").handler(staticHandler);

    // Disable caching so you don't need to clear cache everytime yaml changes.
    StaticHandler swaggerHandler =
        StaticHandler.create().setWebRoot("webroot/swagger").setCachingEnabled(false);
    router.route("/doc/*").handler(swaggerHandler);
  }
}
