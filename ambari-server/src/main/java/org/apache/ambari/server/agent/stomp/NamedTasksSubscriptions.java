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
package org.apache.ambari.server.agent.stomp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;

@Singleton
public class NamedTasksSubscriptions {
  private static Logger LOG = LoggerFactory.getLogger(NamedTasksSubscriptions.class);

  private ConcurrentHashMap<String, List<Long>> taskIds = new ConcurrentHashMap<>();
  private final Pattern pattern = Pattern.compile("^/events/tasks/(\\d*)$");

  public void addTaskId(String sessionId, Long taskId) {
    taskIds.putIfAbsent(sessionId, new ArrayList<>());
    taskIds.get(sessionId).add(taskId);
    LOG.info(String.format("DEBUG Task subscription was added for sessionId = %s, taskId = %s", sessionId, taskId));
  }

  public void removeTaskId(String sessionId, Long taskId) {
    taskIds.computeIfPresent(sessionId, (id, tasks) -> {
      tasks.remove(taskId);
      LOG.info(String.format("DEBUG Task subscription was removed for sessionId = %s, taskId = %s", sessionId, taskId));
      return tasks;
    });
  }

  public void removeSession(String sessionId) {
    taskIds.remove(sessionId);
    LOG.info(String.format("DEBUG Task subscriptions were removed for sessionId = %s", sessionId));
  }

  public Long matchDestination(String destination) {
    Matcher m = pattern.matcher(destination);
    if (m.matches()) {
      return Long.parseLong(m.group(1));
    }
    return null;
  }

  public void addDestination(String sessionId, String destination) {
    Long taskId = matchDestination(destination);
    if (taskId != null) {
      addTaskId(sessionId, taskId);
    }
  }

  public void removeDestination(String sessionId, String destination) {
    Long taskId = matchDestination(destination);
    if (taskId != null) {
      removeTaskId(sessionId, taskId);
    }
  }

  public boolean checkTaskId(Long taskId) {
    for (List<Long> ids: taskIds.values()) {
      if (ids.contains(taskId)) {
        return true;
      }
    }
    return false;
  }
}
