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

package org.apache.ambari.server.controller.internal;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Collections;
import java.util.HashMap;

import org.apache.ambari.server.api.services.AmbariMetaInfo;
import org.apache.ambari.server.api.services.stackadvisor.StackAdvisorHelper;
import org.apache.ambari.server.api.services.stackadvisor.StackAdvisorRequest;
import org.apache.ambari.server.api.services.stackadvisor.recommendations.RecommendationResponse;
import org.apache.ambari.server.configuration.Configuration;
import org.apache.ambari.server.controller.AmbariManagementController;
import org.apache.ambari.server.controller.spi.Request;
import org.apache.ambari.server.controller.spi.RequestStatus;
import org.apache.ambari.server.state.Clusters;
import org.junit.Test;

public class RecommendationResourceProviderTest {

  @Test
  //TODO not completed
  public void testCreateConfigurationResources() throws Exception {
    StackAdvisorHelper stackAdvisorHelper = createMock(StackAdvisorHelper.class);
    Configuration configuration = createMock(Configuration.class);
    Clusters clusters = createMock(Clusters.class);
    AmbariMetaInfo ambariMetaInfo = createMock(AmbariMetaInfo.class);

    RecommendationResourceProvider provider = partialMockBuilder(RecommendationResourceProvider.class)
        .withConstructor(AmbariManagementController.class)
        .withArgs(createMock(AmbariManagementController.class))
        .addMockedMethod("prepareStackAdvisorRequest", Request.class)
        .createMock();
    RecommendationResourceProvider.init(stackAdvisorHelper, configuration, clusters, ambariMetaInfo);

    StackAdvisorRequest stackAdvisorRequest = StackAdvisorRequest.StackAdvisorRequestBuilder.
        forStack(null, null).ofType(StackAdvisorRequest.StackAdvisorRequestType.CONFIGURATIONS).
        build();

    Request request = createMock(Request.class);
    expect(provider.prepareStackAdvisorRequest(eq(request))).andReturn(stackAdvisorRequest);

    RecommendationResponse response = new RecommendationResponse();
    RecommendationResponse.Recommendation recommendation = new RecommendationResponse.Recommendation();

    RecommendationResponse.ConfigGroup configGroup1 = new RecommendationResponse.ConfigGroup();
    recommendation.setConfigGroups(Collections.singleton(configGroup1));

    RecommendationResponse.Blueprint blueprint = new RecommendationResponse.Blueprint();
    blueprint.setConfigurations(new HashMap<>());
    recommendation.setBlueprint(blueprint);
    response.setRecommendations(recommendation);
    expect(stackAdvisorHelper.recommend(anyObject(StackAdvisorRequest.class))).andReturn(response).anyTimes();

    replay(provider, request, stackAdvisorHelper);

    RequestStatus requestStatus = provider.createResources(request);

    verify(provider, request, stackAdvisorHelper);
  }
}
