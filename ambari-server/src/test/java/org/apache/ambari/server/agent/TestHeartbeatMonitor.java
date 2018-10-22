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
package org.apache.ambari.server.agent;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.H2DatabaseCleaner;
import org.apache.ambari.server.Role;
import org.apache.ambari.server.actionmanager.ActionManager;
import org.apache.ambari.server.api.services.AmbariMetaInfo;
import org.apache.ambari.server.configuration.Configuration;
import org.apache.ambari.server.orm.GuiceJpaInitializer;
import org.apache.ambari.server.orm.InMemoryDefaultTestModule;
import org.apache.ambari.server.orm.OrmTestHelper;
import org.apache.ambari.server.orm.entities.RepositoryVersionEntity;
import org.apache.ambari.server.state.Cluster;
import org.apache.ambari.server.state.Clusters;
import org.apache.ambari.server.state.Host;
import org.apache.ambari.server.state.HostState;
import org.apache.ambari.server.state.Service;
import org.apache.ambari.server.state.ServiceComponent;
import org.apache.ambari.server.state.ServiceComponentHost;
import org.apache.ambari.server.state.StackId;
import org.apache.ambari.server.state.State;
import org.apache.ambari.server.state.fsm.InvalidStateTransitionException;
import org.apache.ambari.server.state.svccomphost.ServiceComponentHostDisableEvent;
import org.apache.ambari.server.state.svccomphost.ServiceComponentHostInstallEvent;
import org.apache.ambari.server.state.svccomphost.ServiceComponentHostOpSucceededEvent;
import org.apache.ambari.server.state.svccomphost.ServiceComponentHostStartedEvent;
import org.apache.ambari.server.topology.TopologyManager;
import org.apache.ambari.server.utils.StageUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestHeartbeatMonitor {

  private static Injector injector;

  private String hostname1 = "host1";
  private String hostname2 = "host2";
  private String clusterName = "cluster1";
  private String serviceName = "HDFS";
  private static AmbariMetaInfo ambariMetaInfo;
  private static OrmTestHelper helper;

  private static final Logger LOG =
          LoggerFactory.getLogger(TestHeartbeatMonitor.class);

  @Before
  public void setup() throws Exception {
    injector = Guice.createInjector(new InMemoryDefaultTestModule());
    injector.getInstance(GuiceJpaInitializer.class);
    helper = injector.getInstance(OrmTestHelper.class);
    ambariMetaInfo = injector.getInstance(AmbariMetaInfo.class);
    StageUtils.setTopologyManager(injector.getInstance(TopologyManager.class));
    StageUtils.setConfiguration(injector.getInstance(Configuration.class));
  }

  @After
  public void teardown() throws AmbariException, SQLException {
    H2DatabaseCleaner.clearDatabaseAndStopPersistenceService(injector);
  }

  private void setOsFamily(Host host, String osFamily, String osVersion) {
    Map<String, String> hostAttributes = new HashMap<>();
    hostAttributes.put("os_family", osFamily);
    hostAttributes.put("os_release_version", osVersion);

    host.setHostAttributes(hostAttributes);
  }

  @Test
  public void testHeartbeatLoss() throws AmbariException, InterruptedException,
      InvalidStateTransitionException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Clusters fsm = injector.getInstance(Clusters.class);
    String hostname = "host1";
    fsm.addHost(hostname);
    ActionManager am = mock(ActionManager.class);
    HeartbeatMonitor hm = new HeartbeatMonitor(fsm, am, 10, injector);
    HeartBeatHandler handler = new HeartBeatHandler(fsm, am, injector);
    Register reg = new Register();
    reg.setHostname(hostname);
    reg.setResponseId(12);
    reg.setTimestamp(System.currentTimeMillis() - 300);
    reg.setAgentVersion(ambariMetaInfo.getServerVersion());
    HostInfo hi = new HostInfo();
    hi.setOS("Centos5");
    reg.setHardwareProfile(hi);
    handler.handleRegistration(reg);
    HeartBeat hb = new HeartBeat();
    hb.setHostname(hostname);
    hb.setNodeStatus(new HostStatus(HostStatus.Status.HEALTHY, "cool"));
    hb.setTimestamp(System.currentTimeMillis());
    hb.setResponseId(12);
    handler.handleHeartBeat(hb);
    Method doWorkMethod = HeartbeatMonitor.class.getDeclaredMethod("doWork");
    doWorkMethod.setAccessible(true);

    doWorkMethod.invoke(hm);
    assertEquals(fsm.getHost(hostname).getState(), HostState.HEARTBEAT_LOST);
  }

  @Test
  public void testHeartbeatLossWithComponent() throws AmbariException, InterruptedException,
      InvalidStateTransitionException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    StackId stackId = new StackId("HDP-0.1");
    Clusters clusters = injector.getInstance(Clusters.class);
    clusters.addHost(hostname1);
    setOsFamily(clusters.getHost(hostname1), "redhat", "6.3");

    clusters.addCluster(clusterName, stackId);
    Cluster cluster = clusters.getCluster(clusterName);

    RepositoryVersionEntity repositoryVersion = helper.getOrCreateRepositoryVersion(stackId,
        stackId.getStackVersion());

    Set<String> hostNames = new HashSet<String>(){{
      add(hostname1);
     }};

    clusters.mapAndPublishHostsToCluster(hostNames, clusterName);

    Service hdfs = cluster.addService(serviceName, repositoryVersion);
    hdfs.addServiceComponent(Role.DATANODE.name());
    hdfs.getServiceComponent(Role.DATANODE.name()).addServiceComponentHost(hostname1);
    hdfs.addServiceComponent(Role.NAMENODE.name());
    hdfs.getServiceComponent(Role.NAMENODE.name()).addServiceComponentHost(hostname1);
    hdfs.addServiceComponent(Role.SECONDARY_NAMENODE.name());
    hdfs.getServiceComponent(Role.SECONDARY_NAMENODE.name()).addServiceComponentHost(hostname1);
    hdfs.addServiceComponent(Role.HDFS_CLIENT.name());
    hdfs.getServiceComponent(Role.HDFS_CLIENT.name()).addServiceComponentHost(hostname1);

    ActionManager am = mock(ActionManager.class);
    HeartbeatMonitor hm = new HeartbeatMonitor(clusters, am, 10, injector);
    HeartBeatHandler handler = new HeartBeatHandler(clusters, am, injector);

    Register reg = new Register();
    reg.setHostname(hostname1);
    reg.setResponseId(12);
    reg.setTimestamp(System.currentTimeMillis() - 300);
    reg.setAgentVersion(ambariMetaInfo.getServerVersion());
    HostInfo hi = new HostInfo();
    hi.setOS("Centos5");
    reg.setHardwareProfile(hi);
    handler.handleRegistration(reg);

    cluster = clusters.getClustersForHost(hostname1).iterator().next();
    for (ServiceComponentHost sch : cluster.getServiceComponentHosts(hostname1)) {
      if (sch.getServiceComponentName().equals("NAMENODE")) {
        // installing
        sch.handleEvent(new ServiceComponentHostInstallEvent(
            sch.getServiceComponentName(), sch.getHostName(), System.currentTimeMillis(), "HDP-0.1"));

        // installed
        sch.handleEvent(new ServiceComponentHostOpSucceededEvent(sch.getServiceComponentName(),
            sch.getHostName(), System.currentTimeMillis()));

        // started
        sch.handleEvent(new ServiceComponentHostStartedEvent(sch.getServiceComponentName(),
            sch.getHostName(), System.currentTimeMillis()));
      }
      else if (sch.getServiceComponentName().equals("DATANODE")) {
        // installing
        sch.handleEvent(new ServiceComponentHostInstallEvent(
            sch.getServiceComponentName(), sch.getHostName(), System.currentTimeMillis(), "HDP-0.1"));
      } else if (sch.getServiceComponentName().equals("SECONDARY_NAMENODE"))  {
        // installing
        sch.handleEvent(new ServiceComponentHostInstallEvent(
          sch.getServiceComponentName(), sch.getHostName(), System.currentTimeMillis(), "HDP-0.1"));

        // installed
        sch.handleEvent(new ServiceComponentHostOpSucceededEvent(sch.getServiceComponentName(),
          sch.getHostName(), System.currentTimeMillis()));

        // disabled
        sch.handleEvent(new ServiceComponentHostDisableEvent(sch.getServiceComponentName(),
          sch.getHostName(), System.currentTimeMillis()));
      }
    }

    HeartBeat hb = new HeartBeat();
    hb.setHostname(hostname1);
    hb.setNodeStatus(new HostStatus(HostStatus.Status.HEALTHY, "cool"));
    hb.setTimestamp(System.currentTimeMillis());
    hb.setResponseId(12);
    handler.handleHeartBeat(hb);

    Method doWorkMethod = HeartbeatMonitor.class.getDeclaredMethod("doWork");
    doWorkMethod.setAccessible(true);

    doWorkMethod.invoke(hm);

    cluster = clusters.getClustersForHost(hostname1).iterator().next();
    for (ServiceComponentHost sch : cluster.getServiceComponentHosts(hostname1)) {
      Service s = cluster.getService(sch.getServiceName());
      ServiceComponent sc = s.getServiceComponent(sch.getServiceComponentName());
      if (sch.getServiceComponentName().equals("NAMENODE")) {
        assertEquals(sch.getServiceComponentName(), State.UNKNOWN, sch.getState());
      } else if (sch.getServiceComponentName().equals("DATANODE")) {
        assertEquals(sch.getServiceComponentName(), State.INSTALLING, sch.getState());
      } else if (sc.isClientComponent()) {
        assertEquals(sch.getServiceComponentName(), State.INIT, sch.getState());
      } else if (sch.getServiceComponentName().equals("SECONDARY_NAMENODE")) {
        assertEquals(sch.getServiceComponentName(), State.DISABLED,
          sch.getState());
      }
    }
  }
}
