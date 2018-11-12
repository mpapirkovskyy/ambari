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
package org.apache.ambari.server.agent.stomp;

import java.util.Collections;
import java.util.HashSet;

import org.apache.ambari.server.agent.stomp.dto.TopologyCluster;
import org.apache.ambari.server.agent.stomp.dto.TopologyHost;
import org.apache.ambari.server.agent.stomp.dto.TopologyUpdateHandlingReport;
import org.apache.ambari.server.events.UpdateEventType;
import org.junit.Assert;
import org.junit.Test;

public class TopologyClusterTest {

  @Test
  public void testHandlingReportHostsUpdateAdd() {
    TopologyHost dummyHost = new TopologyHost(1L, "hostName1");
    TopologyHost hostToAddition = new TopologyHost(2L, "hostName2");

    TopologyCluster topologyCluster = new TopologyCluster(new HashSet<>(), new HashSet(){{add(dummyHost);}});

    TopologyUpdateHandlingReport report = new TopologyUpdateHandlingReport();

    topologyCluster.update(Collections.emptySet(), Collections.singleton(hostToAddition), UpdateEventType.UPDATE, report);

    Assert.assertEquals(1L, report.getUpdatedHostNames().size());
  }
}
