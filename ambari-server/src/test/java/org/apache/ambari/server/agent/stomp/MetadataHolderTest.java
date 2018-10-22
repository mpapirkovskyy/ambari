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
package org.apache.ambari.server.agent.stomp;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.ClusterNotFoundException;
import org.apache.ambari.server.agent.stomp.dto.MetadataCluster;
import org.apache.ambari.server.events.MetadataUpdateEvent;
import org.apache.ambari.server.events.UpdateEventType;
import org.apache.ambari.server.events.publishers.AmbariEventPublisher;
import org.apache.ambari.server.state.SecurityType;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;

import com.google.common.collect.Sets;

public class MetadataHolderTest {

  @Test
  public void testGetDeleteMetadata() throws AmbariException {
    MetadataHolder metadataHolder = new MetadataHolder(createNiceMock(AmbariEventPublisher.class));

    MetadataUpdateEvent deleteMetadataUpdateEvent = metadataHolder.getDeleteMetadata(1L);
    assertNotNull(deleteMetadataUpdateEvent.getMetadataClusters());
    assertEquals(UpdateEventType.DELETE, deleteMetadataUpdateEvent.getEventType());
    assertEquals(1, deleteMetadataUpdateEvent.getMetadataClusters().size());
  }

  @Test
  public void testMetadataDeleteExistent() throws AmbariException, NoSuchFieldException, IllegalAccessException {
    MetadataHolder metadataHolder = new MetadataHolder(createNiceMock(AmbariEventPublisher.class));
    String CLUSTER_ID_TO_DELETE = "1";
    String CLUSTER_ID_STILL_PRESENT = "2";

    MetadataUpdateEvent existentUpdate = createSimpleUpdateEvent(UpdateEventType.CREATE,
        Sets.newHashSet(CLUSTER_ID_TO_DELETE, CLUSTER_ID_STILL_PRESENT));
    MetadataUpdateEvent deleteUpdate = createSimpleUpdateEvent(UpdateEventType.DELETE, Sets.newHashSet(CLUSTER_ID_TO_DELETE));

    setCurrentData(metadataHolder, existentUpdate);

    boolean updated = metadataHolder.handleUpdate(deleteUpdate);

    assertTrue(updated);
    assertNotNull(metadataHolder.getData().getMetadataClusters());
    assertEquals(1, metadataHolder.getData().getMetadataClusters().size());
    assertTrue(metadataHolder.getData().getMetadataClusters().keySet().contains(CLUSTER_ID_STILL_PRESENT));
  }

  @Test(expected = ClusterNotFoundException.class)
  public void testMetadataDeleteNonExistent() throws AmbariException, NoSuchFieldException, IllegalAccessException {
    MetadataHolder metadataHolder = new MetadataHolder(createNiceMock(AmbariEventPublisher.class));
    String CLUSTER_ID_TO_DELETE = "1";
    String CLUSTER_ID_STILL_PRESENT = "2";

    MetadataUpdateEvent existentUpdate = createSimpleUpdateEvent(UpdateEventType.CREATE, Sets.newHashSet(CLUSTER_ID_STILL_PRESENT));
    MetadataUpdateEvent deleteUpdate = createSimpleUpdateEvent(UpdateEventType.DELETE, Sets.newHashSet(CLUSTER_ID_TO_DELETE));

    setCurrentData(metadataHolder, existentUpdate);

    metadataHolder.handleUpdate(deleteUpdate);
  }

  @Test
  public void testEmptyUpdate() throws NoSuchFieldException, IllegalAccessException, AmbariException {
    MetadataHolder metadataHolder = new MetadataHolder(createNiceMock(AmbariEventPublisher.class));

    MetadataUpdateEvent existentUpdate = createSimpleUpdateEvent(UpdateEventType.CREATE, Sets.newHashSet("1", "2"));
    MetadataUpdateEvent validUpdate = createSimpleUpdateEvent(UpdateEventType.UPDATE, Sets.newHashSet());

    setCurrentData(metadataHolder, existentUpdate);

    boolean updated = metadataHolder.handleUpdate(validUpdate);

    assertFalse(updated);
  }

  @Test
  public void testUpdateNonExistent() throws NoSuchFieldException, IllegalAccessException, AmbariException {
    MetadataHolder metadataHolder = new MetadataHolder(createNiceMock(AmbariEventPublisher.class));

    MetadataUpdateEvent existentUpdate = createSimpleUpdateEvent(UpdateEventType.CREATE, Sets.newHashSet("1"));
    MetadataUpdateEvent validUpdate = createSimpleUpdateEvent(UpdateEventType.UPDATE, Sets.newHashSet("2"));

    setCurrentData(metadataHolder, existentUpdate);

    boolean updated = metadataHolder.handleUpdate(validUpdate);

    assertTrue(updated);
    assertNotNull(metadataHolder.getData().getMetadataClusters());
    assertEquals(2, metadataHolder.getData().getMetadataClusters().size());
    assertTrue(metadataHolder.getData().getMetadataClusters().keySet().contains("1"));
    assertTrue(metadataHolder.getData().getMetadataClusters().keySet().contains("2"));
  }

  @Test
  public void testUpdateExistent() throws NoSuchFieldException, IllegalAccessException, AmbariException {
    MetadataHolder metadataHolder = new MetadataHolder(createNiceMock(AmbariEventPublisher.class));

    SortedMap<String, MetadataCluster> existentClusters = new TreeMap<>();
    MetadataCluster existentCluster = createMock(MetadataCluster.class);
    existentClusters.put("1", existentCluster);
    MetadataUpdateEvent existentUpdate = new MetadataUpdateEvent(existentClusters, null, null, UpdateEventType.CREATE);

    SortedMap<String, MetadataCluster> clustersToUpdate = new TreeMap<>();
    MetadataCluster clusterToUpdate = new MetadataCluster(SecurityType.NONE,
        MapUtils.EMPTY_SORTED_MAP,
        false,
        MapUtils.EMPTY_SORTED_MAP,
        MapUtils.EMPTY_SORTED_MAP);
    clustersToUpdate.put("1", clusterToUpdate);
    MetadataUpdateEvent validUpdate = new MetadataUpdateEvent(clustersToUpdate, null, null, UpdateEventType.UPDATE);

    setCurrentData(metadataHolder, existentUpdate);

    expect(existentCluster.updateClusterLevelParams(anyObject(SortedMap.class))).andReturn(true);
    expect(existentCluster.updateServiceLevelParams(anyObject(SortedMap.class), eq(false))).andReturn(true);
    expect(existentCluster.getStatusCommandsToRun()).andReturn(new TreeSet<>()).atLeastOnce();

    replay(existentCluster);

    boolean updated = metadataHolder.handleUpdate(validUpdate);

    assertTrue(updated);
    assertNotNull(metadataHolder.getData().getMetadataClusters());
    assertEquals(1, metadataHolder.getData().getMetadataClusters().size());

    verify(existentCluster);
  }

  private MetadataUpdateEvent createSimpleUpdateEvent(UpdateEventType eventType, Set<String> clusterIds) {
    SortedMap<String, MetadataCluster> clusters = new TreeMap<>();
    for (String clusterId : clusterIds) {
      clusters.put(clusterId, null);
    }
    MetadataUpdateEvent updateEvent = new MetadataUpdateEvent(clusters, null, null, eventType);
    return updateEvent;
  }

  private void setCurrentData(MetadataHolder metadataHolder, MetadataUpdateEvent existentData)
      throws NoSuchFieldException, IllegalAccessException {
    Field dataField = MetadataHolder.class.getSuperclass().getDeclaredField("data");
    dataField.setAccessible(true);
    dataField.set(metadataHolder, existentData);
  }

}
