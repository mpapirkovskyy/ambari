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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.TreeMap;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.events.MetadataUpdateEvent;
import org.apache.ambari.server.events.UpdateEventType;
import org.apache.ambari.server.events.publishers.AmbariEventPublisher;
import org.apache.ambari.server.events.publishers.STOMPUpdatePublisher;
import org.apache.commons.collections.MapUtils;
import org.easymock.EasyMock;
import org.junit.Test;

public class AgentClusterDataHolderTest {

  @Test
  public void testGetUpdateIfChanged() throws AmbariException {
    MetadataHolder dataHolder = EasyMock.partialMockBuilder(MetadataHolder.class)
        .withConstructor(AmbariEventPublisher.class)
        .withArgs(EasyMock.createStrictMock(AmbariEventPublisher.class))
        .addMockedMethod("getCurrentData")
        .createNiceMock();

    TreeMap<String, String> dummyAmbariLevelParams = new TreeMap<>();
    dummyAmbariLevelParams.put("propName", "propValue");
    MetadataUpdateEvent serverMetadataState = new MetadataUpdateEvent(new TreeMap<>(), dummyAmbariLevelParams,
        MapUtils.EMPTY_SORTED_MAP, UpdateEventType.UPDATE);
    expect(dataHolder.getCurrentData()).andReturn(serverMetadataState).once();

    replay(dataHolder);

    // agent does not have hash and sends empty
    MetadataUpdateEvent initUpdate = dataHolder.getUpdateIfChanged("");

    assertNotNull(initUpdate.getHash());
    assertEquals(dummyAmbariLevelParams, initUpdate.getMetadataClusters().get("-1").getClusterLevelParams());

    String initHash = initUpdate.getHash();

    // agent sends current hash
    MetadataUpdateEvent otherUpdate = dataHolder.getUpdateIfChanged(initHash);
    assertEquals(dataHolder.getEmptyData(), otherUpdate);

    // agent sends invalid hash
    otherUpdate = dataHolder.getUpdateIfChanged("invalidhash");
    assertFalse(dataHolder.getEmptyData().equals(otherUpdate));
    assertEquals(dummyAmbariLevelParams, initUpdate.getMetadataClusters().get("-1").getClusterLevelParams());

    EasyMock.verify(dataHolder);
  }

  @Test
  public void testUpdateData() throws AmbariException {
    MetadataHolder dataHolder = EasyMock.partialMockBuilder(MetadataHolder.class)
        .withConstructor(AmbariEventPublisher.class)
        .withArgs(createStrictMock(AmbariEventPublisher.class))
        .addMockedMethod("getCurrentData")
        .addMockedMethod("handleUpdate")
        .createNiceMock();

    STOMPUpdatePublisher stompUpdatePublisher = createStrictMock(STOMPUpdatePublisher.class);
    dataHolder.STOMPUpdatePublisher = stompUpdatePublisher;

    // on not handled update
    expect(dataHolder.getCurrentData()).andReturn(new MetadataUpdateEvent(new TreeMap<>(), new TreeMap<>(),
        MapUtils.EMPTY_SORTED_MAP, UpdateEventType.UPDATE));
    expect(dataHolder.handleUpdate(anyObject(MetadataUpdateEvent.class))).andReturn(false);

    replay(dataHolder, stompUpdatePublisher);

    dataHolder.updateData(new MetadataUpdateEvent(new TreeMap<>(), new TreeMap<>(),
        MapUtils.EMPTY_SORTED_MAP, UpdateEventType.UPDATE));
    // update without current retrieving should regenerate hash if empty
    assertNotNull(dataHolder.getData().getHash());

    verify(dataHolder, stompUpdatePublisher);

    // on handled update
    reset(dataHolder, stompUpdatePublisher);

    expect(dataHolder.handleUpdate(anyObject(MetadataUpdateEvent.class))).andReturn(true);

    stompUpdatePublisher.publish(anyObject(MetadataUpdateEvent.class));
    expectLastCall();

    replay(dataHolder, stompUpdatePublisher);

    dataHolder.updateData(new MetadataUpdateEvent(new TreeMap<>(), new TreeMap<>(),
        MapUtils.EMPTY_SORTED_MAP, UpdateEventType.UPDATE));
    assertNotNull(dataHolder.getData().getHash());

    verify(dataHolder, stompUpdatePublisher);
  }
}
