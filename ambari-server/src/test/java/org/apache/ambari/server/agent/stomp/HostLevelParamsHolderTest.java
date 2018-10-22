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

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.agent.RecoveryConfig;
import org.apache.ambari.server.agent.RecoveryConfigHelper;
import org.apache.ambari.server.agent.stomp.dto.HostLevelParamsCluster;
import org.apache.ambari.server.controller.AmbariManagementController;
import org.apache.ambari.server.events.HostLevelParamsUpdateEvent;
import org.apache.ambari.server.events.publishers.AmbariEventPublisher;
import org.apache.ambari.server.orm.InMemoryDefaultTestModule;
import org.apache.ambari.server.state.Cluster;
import org.apache.ambari.server.state.Clusters;
import org.apache.ambari.server.state.Host;
import org.apache.commons.collections.MapUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;

public class HostLevelParamsHolderTest {
  private Injector injector;
  private Clusters clusters;
  private Host host;
  private Cluster cluster1;
  private Cluster cluster2;
  private AmbariManagementController controller;
  private RecoveryConfigHelper recoveryConfigHelper;

  private static final Long HOST_ID = 1L;
  private static final String HOST_NAME = "hostName";
  private static final Long CLUSTER_ID_1 = 2L;
  private static final String CLUSTER_NAME_1 = "clusterName1";
  private static final Long CLUSTER_ID_2 = 3L;
  private static final String CLUSTER_NAME_2 = "clusterName2";

  @Before
  public void setupTest() throws AmbariException {
    injector = Guice.createInjector(Modules.override(new InMemoryDefaultTestModule()).with(new MockModule()));

    clusters = injector.getInstance(Clusters.class);
    host = createMock(Host.class);
    cluster1 = createMock(Cluster.class);
    cluster2 = createMock(Cluster.class);
    controller = injector.getInstance(AmbariManagementController.class);
    recoveryConfigHelper = injector.getInstance(RecoveryConfigHelper.class);

    RecoveryConfig recoveryConfig = createStrictMock(RecoveryConfig.class);

    expect(host.getHostId()).andReturn(HOST_ID).anyTimes();
    expect(host.getHostName()).andReturn(HOST_NAME).anyTimes();

    expect(cluster1.getClusterId()).andReturn(CLUSTER_ID_1).anyTimes();
    expect(cluster1.getClusterName()).andReturn(CLUSTER_NAME_1).anyTimes();
    expect(cluster2.getClusterId()).andReturn(CLUSTER_ID_2).anyTimes();
    expect(cluster2.getClusterName()).andReturn(CLUSTER_NAME_2).anyTimes();

    expect(controller.getBlueprintProvisioningStates(anyLong(), anyLong())).andReturn(MapUtils.EMPTY_MAP).anyTimes();

    expect(clusters.getHostById(eq(HOST_ID))).andReturn(host);
    Set<Cluster> clusterSet = new HashSet<>();
    clusterSet.add(cluster1);
    clusterSet.add(cluster2);
    expect(clusters.getClustersForHost(eq(HOST_NAME))).andReturn(clusterSet);

    expect(recoveryConfigHelper.getRecoveryConfig(eq(CLUSTER_NAME_1), eq(HOST_NAME))).andReturn(recoveryConfig).anyTimes();
    expect(recoveryConfigHelper.getRecoveryConfig(eq(CLUSTER_NAME_2), eq(HOST_NAME))).andReturn(recoveryConfig).anyTimes();

  }

  @Test
  public void testHandleUpdateEmptyCurrent() {
    HostLevelParamsUpdateEvent current = new HostLevelParamsUpdateEvent(HOST_ID, Collections.emptyMap());
    Map<String, HostLevelParamsCluster> clusters = new HashMap<>();
    HostLevelParamsCluster cluster = new HostLevelParamsCluster(new RecoveryConfig(null), Collections.emptyMap());
    clusters.put("1", cluster);
    HostLevelParamsUpdateEvent update = new HostLevelParamsUpdateEvent(HOST_ID, clusters);

    HostLevelParamsHolder levelParamsHolder = new HostLevelParamsHolder(createNiceMock(AmbariEventPublisher.class));
    HostLevelParamsUpdateEvent result = levelParamsHolder.handleUpdate(current, update);

    assertFalse(result == update);
    assertFalse(result == current);
    assertEquals(result, update);
  }

  @Test
  public void testHandleUpdateEmptyUpdate() {
    Map<String, HostLevelParamsCluster> clusters = new HashMap<>();
    HostLevelParamsCluster cluster = new HostLevelParamsCluster(new RecoveryConfig(null), Collections.emptyMap());
    clusters.put("1", cluster);
    HostLevelParamsUpdateEvent current = new HostLevelParamsUpdateEvent(HOST_ID, clusters);
    HostLevelParamsUpdateEvent update = new HostLevelParamsUpdateEvent(HOST_ID, Collections.emptyMap());

    HostLevelParamsHolder levelParamsHolder = new HostLevelParamsHolder(createNiceMock(AmbariEventPublisher.class));
    HostLevelParamsUpdateEvent result = levelParamsHolder.handleUpdate(current, update);

    assertFalse(result == update);
    assertFalse(result == current);
    assertEquals(result, null);
  }

  @Test
  public void testGetCurrentDataExcludeNonExistentCluster() throws AmbariException {
    final Long NON_EXISTENT_CLUSTER_ID = 4L;
    replay(clusters, host, cluster1, cluster2, controller, recoveryConfigHelper);

    HostLevelParamsUpdateEvent updateEvent = injector.getInstance(HostLevelParamsHolder.class).getCurrentDataExcludeCluster(HOST_ID, NON_EXISTENT_CLUSTER_ID);
    assertNotNull(updateEvent);
    assertNotNull(updateEvent.getHostLevelParamsClusters());
    assertEquals(HOST_ID, updateEvent.getHostId());
    assertEquals(2, updateEvent.getHostLevelParamsClusters().size());
    assertTrue(updateEvent.getHostLevelParamsClusters().containsKey(Long.toString(CLUSTER_ID_1)));
    assertTrue(updateEvent.getHostLevelParamsClusters().containsKey(Long.toString(CLUSTER_ID_2)));

    verify(clusters, host, cluster1, cluster2, controller, recoveryConfigHelper);
  }

  @Test
  public void testGetCurrentDataExcludeNullCluster() throws AmbariException {
    replay(clusters, host, cluster1, cluster2, controller, recoveryConfigHelper);

    HostLevelParamsUpdateEvent updateEvent = injector.getInstance(HostLevelParamsHolder.class).getCurrentDataExcludeCluster(HOST_ID, null);
    assertNotNull(updateEvent);
    assertNotNull(updateEvent.getHostLevelParamsClusters());
    assertEquals(HOST_ID, updateEvent.getHostId());
    assertEquals(2, updateEvent.getHostLevelParamsClusters().size());
    assertTrue(updateEvent.getHostLevelParamsClusters().containsKey(Long.toString(CLUSTER_ID_1)));
    assertTrue(updateEvent.getHostLevelParamsClusters().containsKey(Long.toString(CLUSTER_ID_2)));

    verify(clusters, host, cluster1, cluster2, controller, recoveryConfigHelper);
  }

  @Test
  public void testGetCurrentDataExcludeCluster() throws AmbariException {
    replay(clusters, host, cluster1, cluster2, controller, recoveryConfigHelper);

    HostLevelParamsUpdateEvent updateEvent = injector.getInstance(HostLevelParamsHolder.class).getCurrentDataExcludeCluster(HOST_ID, CLUSTER_ID_1);
    assertNotNull(updateEvent);
    assertNotNull(updateEvent.getHostLevelParamsClusters());
    assertEquals(HOST_ID, updateEvent.getHostId());
    assertEquals(1, updateEvent.getHostLevelParamsClusters().size());
    assertTrue(updateEvent.getHostLevelParamsClusters().containsKey(Long.toString(CLUSTER_ID_2)));

    verify(clusters, host, cluster1, cluster2, controller, recoveryConfigHelper);
  }

  @Test
  public void testGetEmptyData() {
    HostLevelParamsUpdateEvent emptyUpdate = injector.getInstance(HostLevelParamsHolder.class).getEmptyData();
    assertNull(emptyUpdate.getHostId());
    assertNull(emptyUpdate.getHostLevelParamsClusters());
  }

  @Test
  public void testHandleUpdateNoChanges() {
    Map<String, HostLevelParamsCluster> currentClusters = new HashMap<>();
    HostLevelParamsCluster currentCluster = new HostLevelParamsCluster(new RecoveryConfig(null), Collections.emptyMap());
    currentClusters.put("1", currentCluster);
    HostLevelParamsUpdateEvent current = new HostLevelParamsUpdateEvent(HOST_ID, currentClusters);

    Map<String, HostLevelParamsCluster> updateClusters = new HashMap<>();
    HostLevelParamsCluster updateCluster = new HostLevelParamsCluster(new RecoveryConfig(null), Collections.emptyMap());
    updateClusters.put("1", updateCluster);
    HostLevelParamsUpdateEvent update = new HostLevelParamsUpdateEvent(HOST_ID, updateClusters);

    HostLevelParamsHolder levelParamsHolder = new HostLevelParamsHolder(createNiceMock(AmbariEventPublisher.class));
    HostLevelParamsUpdateEvent result = levelParamsHolder.handleUpdate(current, update);

    assertFalse(result == update);
    assertFalse(result == current);
    assertEquals(result, null);
  }

  @Test
  public void testHandleUpdateOnChanges() {
    Map<String, HostLevelParamsCluster> currentClusters = new HashMap<>();
    HostLevelParamsCluster currentCluster = new HostLevelParamsCluster(new RecoveryConfig(null), Collections.emptyMap());
    currentClusters.put("1", currentCluster);
    HostLevelParamsUpdateEvent current = new HostLevelParamsUpdateEvent(HOST_ID, currentClusters);

    Map<String, HostLevelParamsCluster> updateClusters = new HashMap<>();
    HostLevelParamsCluster updateCluster = new HostLevelParamsCluster(new RecoveryConfig(null), Collections.emptyMap());
    updateClusters.put("2", updateCluster);
    HostLevelParamsUpdateEvent update = new HostLevelParamsUpdateEvent(HOST_ID, updateClusters);

    HostLevelParamsHolder levelParamsHolder = new HostLevelParamsHolder(createNiceMock(AmbariEventPublisher.class));
    HostLevelParamsUpdateEvent result = levelParamsHolder.handleUpdate(current, update);

    assertFalse(result == update);
    assertFalse(result == current);
    assertEquals(2, result.getHostLevelParamsClusters().size());
    assertTrue(result.getHostLevelParamsClusters().containsKey("1"));
    assertTrue(result.getHostLevelParamsClusters().containsKey("2"));
  }

  private class MockModule implements Module {
    /**
     *
     */
    @Override
    public void configure(Binder binder) {
      binder.bind(AmbariManagementController.class).toInstance(createMock(AmbariManagementController.class));
      binder.bind(Clusters.class).toInstance(createMock(Clusters.class));
      binder.bind(RecoveryConfigHelper.class).toInstance(createMock(RecoveryConfigHelper.class));
    }
  }
}
