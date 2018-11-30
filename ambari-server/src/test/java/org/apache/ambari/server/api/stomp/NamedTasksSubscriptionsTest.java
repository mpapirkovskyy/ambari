/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.server.api.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class NamedTasksSubscriptionsTest {
  private static final String SESSION_ID_1 = "fdsg3";
  private static final String SESSION_ID_2 = "idfg6";

  private NamedTasksSubscriptions tasksSubscriptions;

  @Before
  public void setupTest() {
    tasksSubscriptions = new NamedTasksSubscriptions();
    tasksSubscriptions.addTaskId(SESSION_ID_1, 1L);
    tasksSubscriptions.addTaskId(SESSION_ID_1, 5L);
    tasksSubscriptions.addTaskId(SESSION_ID_2, 1L);
    tasksSubscriptions.addTaskId(SESSION_ID_2, 4L);
  }

  @Test
  public void testMatching() {
    assertEquals(1L, tasksSubscriptions.matchDestination("/events/tasks/1").longValue());
    assertNull(tasksSubscriptions.matchDestination("/events/topologies"));
  }

  @Test
  public void testCheckId() {
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertTrue(tasksSubscriptions.checkTaskId(5L));
    assertFalse(tasksSubscriptions.checkTaskId(2L));
  }

  @Test
  public void testRemoveBySessionId() {
    tasksSubscriptions.removeSession(SESSION_ID_1);
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeSession(SESSION_ID_2);
    assertFalse(tasksSubscriptions.checkTaskId(1L));
    assertFalse(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));
  }

  @Test
  public void testRemoveByTaskId() {
    tasksSubscriptions.removeTaskId(SESSION_ID_1, 1L);
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertTrue(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeTaskId(SESSION_ID_1, 5L);
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeTaskId(SESSION_ID_2, 1L);
    assertFalse(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeTaskId(SESSION_ID_2, 4L);
    assertFalse(tasksSubscriptions.checkTaskId(1L));
    assertFalse(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));
  }

  @Test
  public void testRemoveDestination() {
    tasksSubscriptions.removeDestination(SESSION_ID_1, "/events/tasks/1");
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertTrue(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeDestination(SESSION_ID_1, "/events/tasks/5");
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeDestination(SESSION_ID_2, "/events/tasks/1");
    assertFalse(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.removeDestination(SESSION_ID_2, "/events/tasks/4");
    assertFalse(tasksSubscriptions.checkTaskId(1L));
    assertFalse(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));
  }

  @Test
  public void testAddDestination() {
    tasksSubscriptions = new NamedTasksSubscriptions();
    tasksSubscriptions.addDestination(SESSION_ID_1, "/events/tasks/1");
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertFalse(tasksSubscriptions.checkTaskId(4L));
    assertFalse(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.addDestination(SESSION_ID_1, "/events/tasks/5");
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertFalse(tasksSubscriptions.checkTaskId(4L));
    assertTrue(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.addDestination(SESSION_ID_2, "/events/tasks/1");
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertFalse(tasksSubscriptions.checkTaskId(4L));
    assertTrue(tasksSubscriptions.checkTaskId(5L));

    tasksSubscriptions.addDestination(SESSION_ID_2, "/events/tasks/4");
    assertTrue(tasksSubscriptions.checkTaskId(1L));
    assertTrue(tasksSubscriptions.checkTaskId(4L));
    assertTrue(tasksSubscriptions.checkTaskId(5L));
  }
}
