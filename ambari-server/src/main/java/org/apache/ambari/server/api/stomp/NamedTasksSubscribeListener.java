/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.server.api.stomp;

import org.apache.ambari.server.agent.stomp.NamedTasksSubscriptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

@Component
public class NamedTasksSubscribeListener {
  private static Logger LOG = LoggerFactory.getLogger(NamedTasksSubscribeListener.class);

  @Autowired
  private NamedTasksSubscriptions namedTasksSubscriptions;

  @EventListener
  public void subscribe(SessionSubscribeEvent sse)
  {
    MessageHeaders msgHeaders = sse.getMessage().getHeaders();
    StompHeaderAccessor sha = StompHeaderAccessor.wrap(sse.getMessage());
    String sessionId  = (String) msgHeaders.get("simpSessionId");
    String destination  = (String) msgHeaders.get("simpDestination");
    if (sessionId != null && destination != null) {
      LOG.info(String.format("DEBUG API subscribe was arrived with sessionId = %s and destination = %s",
          sessionId, destination));
      namedTasksSubscriptions.addDestination(sessionId, destination);
    }
  }

  @EventListener
  public void unsubscribe(SessionUnsubscribeEvent suse)
  {
    MessageHeaders msgHeaders = suse.getMessage().getHeaders();
    String sessionId  = (String) msgHeaders.get("simpSessionId");
    String destination  = (String) msgHeaders.get("simpDestination");
    if (sessionId != null && destination != null) {
      LOG.info(String.format("DEBUG API unsubscribe was arrived with sessionId = %s and destination = %s",
          sessionId, destination));
      namedTasksSubscriptions.removeDestination(sessionId, destination);
    }
  }

  @EventListener
  public void disconnect(SessionDisconnectEvent sde)
  {
    MessageHeaders msgHeaders = sde.getMessage().getHeaders();
    String sessionId  = (String) msgHeaders.get("simpSessionId");
    if (sessionId != null) {
      LOG.info(String.format("DEBUG API disconnect was arrived with sessionId = %s",
          sessionId));
      namedTasksSubscriptions.removeSession(sessionId);
    }
  }
}
