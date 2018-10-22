/*
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
package org.apache.ambari.server.agent.stomp;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.events.HostLevelParamsUpdateEvent;
import org.apache.ambari.server.events.publishers.AmbariEventPublisher;
import org.apache.ambari.server.events.publishers.STOMPUpdatePublisher;
import org.apache.commons.collections.MapUtils;
import org.easymock.EasyMock;
import org.junit.Test;

public class AgentHostDataHolderTest {

  private static final Long HOST_ID = 1L;

  @Test
  public void testGetUpdateIfChanged() throws AmbariException {
    HostLevelParamsHolder hostHolder = EasyMock.partialMockBuilder(HostLevelParamsHolder.class)
        .withConstructor(AmbariEventPublisher.class)
        .withArgs(EasyMock.createStrictMock(AmbariEventPublisher.class))
        .addMockedMethod("getCurrentData")
        .createNiceMock();

    HostLevelParamsUpdateEvent hostUpdateEvent = new HostLevelParamsUpdateEvent(HOST_ID, MapUtils.EMPTY_MAP);
    expect(hostHolder.getCurrentData(eq(HOST_ID))).andReturn(hostUpdateEvent).once();

    replay(hostHolder);

    // agent does not have hash and sends empty
    HostLevelParamsUpdateEvent initUpdate = hostHolder.getUpdateIfChanged("", HOST_ID);

    assertNotNull(initUpdate.getHash());
    assertEquals(hostUpdateEvent, initUpdate);

    String initHash = initUpdate.getHash();

    // agent sends current hash
    HostLevelParamsUpdateEvent otherUpdate = hostHolder.getUpdateIfChanged(initHash, HOST_ID);
    assertEquals(hostHolder.getEmptyData(), otherUpdate);

    // agent sends invalid hash
    otherUpdate = hostHolder.getUpdateIfChanged("invalidhash", HOST_ID);
    assertFalse(hostHolder.getEmptyData().equals(otherUpdate));
    assertEquals(hostUpdateEvent, initUpdate);

    EasyMock.verify(hostHolder);
  }

  @Test
  public void testUpdateData() throws AmbariException, NoSuchFieldException, IllegalAccessException {
    HostLevelParamsHolder hostHolder = EasyMock.partialMockBuilder(HostLevelParamsHolder.class)
        .withConstructor(AmbariEventPublisher.class)
        .withArgs(EasyMock.createStrictMock(AmbariEventPublisher.class))
        .addMockedMethod("getCurrentData")
        .addMockedMethod("handleUpdate")
        .createNiceMock();

    STOMPUpdatePublisher stompUpdatePublisher = createStrictMock(STOMPUpdatePublisher.class);
    Field stompUpdatePublisherField = HostLevelParamsHolder.class.getSuperclass().getDeclaredField("STOMPUpdatePublisher");
    stompUpdatePublisherField.setAccessible(true);
    stompUpdatePublisherField.set(hostHolder, stompUpdatePublisher);

    // on not handled update
    expect(hostHolder.getCurrentData(HOST_ID)).andReturn(new HostLevelParamsUpdateEvent(HOST_ID, MapUtils.EMPTY_MAP));
    expect(hostHolder.handleUpdate(anyObject(HostLevelParamsUpdateEvent.class), anyObject(HostLevelParamsUpdateEvent.class))).andReturn(null);

    replay(hostHolder, stompUpdatePublisher);

    HostLevelParamsUpdateEvent hostUpdateEvent = new HostLevelParamsUpdateEvent(HOST_ID, MapUtils.EMPTY_MAP);
    hostHolder.updateData(hostUpdateEvent);
    // update without current retrieving should regenerate hash if empty
    assertNotNull(hostHolder.getData(HOST_ID).getHash());

    verify(hostHolder, stompUpdatePublisher);

    // on handled update
    reset(hostHolder, stompUpdatePublisher);

    expect(hostHolder.handleUpdate(anyObject(HostLevelParamsUpdateEvent.class), anyObject(HostLevelParamsUpdateEvent.class)))
        .andReturn(new HostLevelParamsUpdateEvent(HOST_ID, MapUtils.EMPTY_MAP));

    stompUpdatePublisher.publish(anyObject(HostLevelParamsUpdateEvent.class));
    expectLastCall();

    replay(hostHolder, stompUpdatePublisher);

    hostHolder.updateData(hostUpdateEvent);
    assertNotNull(hostHolder.getData(HOST_ID).getHash());

    verify(hostHolder, stompUpdatePublisher);
  }
}
