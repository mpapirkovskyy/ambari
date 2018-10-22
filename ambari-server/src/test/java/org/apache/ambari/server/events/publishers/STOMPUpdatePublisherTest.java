/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.server.events.publishers;


import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import java.lang.reflect.Field;

import org.apache.ambari.server.events.DefaultMessageEmitter;
import org.apache.ambari.server.events.HostComponentsUpdateEvent;
import org.apache.ambari.server.events.RequestUpdateEvent;
import org.apache.ambari.server.events.STOMPEvent;
import org.apache.ambari.server.events.ServiceUpdateEvent;
import org.junit.Test;

import com.google.common.eventbus.EventBus;

public class STOMPUpdatePublisherTest {

  @Test
  public void testValidUpdates() throws NoSuchFieldException, IllegalAccessException {
    STOMPUpdatePublisher stompUpdatePublisher = new STOMPUpdatePublisher();

    // inject merging publishers
    RequestUpdateEventPublisher requestUpdateEventPublisher = createMock(RequestUpdateEventPublisher.class);
    Field requestUpdateEventPublisherField = STOMPUpdatePublisher.class.getDeclaredField("requestUpdateEventPublisher");
    requestUpdateEventPublisherField.setAccessible(true);
    requestUpdateEventPublisherField.set(stompUpdatePublisher, requestUpdateEventPublisher);

    HostComponentUpdateEventPublisher hostComponentUpdateEventPublisher = createMock(HostComponentUpdateEventPublisher.class);
    Field hostComponentUpdateEventPublisherField = STOMPUpdatePublisher.class.getDeclaredField("hostComponentUpdateEventPublisher");
    hostComponentUpdateEventPublisherField.setAccessible(true);
    hostComponentUpdateEventPublisherField.set(stompUpdatePublisher, hostComponentUpdateEventPublisher);

    ServiceUpdateEventPublisher serviceUpdateEventPublisher = createMock(ServiceUpdateEventPublisher.class);
    Field serviceUpdateEventPublisherField = STOMPUpdatePublisher.class.getDeclaredField("serviceUpdateEventPublisher");
    serviceUpdateEventPublisherField.setAccessible(true);
    serviceUpdateEventPublisherField.set(stompUpdatePublisher, serviceUpdateEventPublisher);

    EventBus agentEventBus = createMock(EventBus.class);
    Field agentEventBusField = STOMPUpdatePublisher.class.getDeclaredField("agentEventBus");
    agentEventBusField.setAccessible(true);
    agentEventBusField.set(stompUpdatePublisher, agentEventBus);

    EventBus apiEventBus = createMock(EventBus.class);
    Field apiEventBusField = STOMPUpdatePublisher.class.getDeclaredField("apiEventBus");
    apiEventBusField.setAccessible(true);
    apiEventBusField.set(stompUpdatePublisher, apiEventBus);


    for (STOMPEvent.Type apiEventType : DefaultMessageEmitter.DEFAULT_API_EVENT_TYPES) {
      reset(agentEventBus, apiEventBus, requestUpdateEventPublisher, hostComponentUpdateEventPublisher,
          serviceUpdateEventPublisher);

      STOMPEvent event;
      if (apiEventType.equals(STOMPEvent.Type.REQUEST)) {
        event = new RequestUpdateEvent(null, null, null);

        requestUpdateEventPublisher.publish((RequestUpdateEvent) event, apiEventBus);
        expectLastCall();
      } else if (apiEventType.equals(STOMPEvent.Type.SERVICE)) {
        event = new ServiceUpdateEvent(null, null, null, null);

        serviceUpdateEventPublisher.publish((ServiceUpdateEvent) event, apiEventBus);
        expectLastCall();
      } else if (apiEventType.equals(STOMPEvent.Type.HOSTCOMPONENT)) {
        event = new HostComponentsUpdateEvent(null);

        hostComponentUpdateEventPublisher.publish((HostComponentsUpdateEvent) event, apiEventBus);
        expectLastCall();
      } else {
        event = new STOMPEvent(apiEventType) {
          @Override
          public Type getType() {
            return super.getType();
          }
        };

        // metadata update is sent both to api and agent publishers
        if (apiEventType.equals(STOMPEvent.Type.METADATA)) {
          agentEventBus.post(event);
          expectLastCall();
        }

        apiEventBus.post(event);
        expectLastCall();
      }

      replay(agentEventBus, apiEventBus, requestUpdateEventPublisher, hostComponentUpdateEventPublisher,
          serviceUpdateEventPublisher);

      stompUpdatePublisher.publish(event);

      verify(agentEventBus, apiEventBus, requestUpdateEventPublisher, hostComponentUpdateEventPublisher,
          serviceUpdateEventPublisher);
    }

    for (STOMPEvent.Type agentEventType : DefaultMessageEmitter.DEFAULT_AGENT_EVENT_TYPES) {
      reset(agentEventBus, apiEventBus, requestUpdateEventPublisher, hostComponentUpdateEventPublisher,
          serviceUpdateEventPublisher);

      STOMPEvent event = new STOMPEvent(agentEventType) {
        @Override
        public Type getType() {
          return super.getType();
        }
      };

      // metadata update is sent both to api and agent publishers
      if (agentEventType.equals(STOMPEvent.Type.METADATA)) {
        apiEventBus.post(event);
        expectLastCall();
      }

      agentEventBus.post(event);
      expectLastCall();

      replay(agentEventBus, apiEventBus, requestUpdateEventPublisher, hostComponentUpdateEventPublisher,
          serviceUpdateEventPublisher);

      stompUpdatePublisher.publish(event);

      verify(agentEventBus, apiEventBus, requestUpdateEventPublisher, hostComponentUpdateEventPublisher,
          serviceUpdateEventPublisher);
    }
  }
}
