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

package org.apache.ambari.server.events.publishers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.eventbus.EventBus;
import com.google.inject.Singleton;

@Singleton
public abstract class BufferedUpdateEventPublisher<T> {

  private static final long TIMEOUT = 1000L;

  /**
   * Means new merge task should be scheduled when set to false,
   * otherwise all events will be processed in the current task scope.
   */
  private final AtomicBoolean collecting = new AtomicBoolean(false);

  /**
   * Means previous merging already completed (true) or not (false).
   * I used to avoid merge tasks collection when merging requires more time than TIMEOUT between tasks.
   */
  private final AtomicBoolean released = new AtomicBoolean(true);
  private final ConcurrentLinkedQueue<T> buffer = new ConcurrentLinkedQueue<>();
  private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

  public void publish(T event, EventBus m_eventBus) {
    if (collecting.get()) {
      buffer.add(event);
    } else {
      buffer.add(event);
      collecting.set(true);
      scheduledExecutorService.schedule(getScheduledPublisher(m_eventBus),
          TIMEOUT, TimeUnit.MILLISECONDS);
    }
  }

  protected ReleasableRunnable getScheduledPublisher(EventBus m_eventBus) {
    return new ReleasableRunnable(m_eventBus);
  }

  protected synchronized List<T> retrieveBuffer() {
    resetCollecting();
    List<T> bufferContent = new ArrayList<>();
    if (released.get()) {
      while (!buffer.isEmpty()) {
        bufferContent.add(buffer.poll());
      }
    }
    return bufferContent;
  }

  protected void resetCollecting() {
    collecting.set(false);
  }

  public abstract void mergeBufferAndPost(List<T> events, EventBus m_eventBus);

  private class ReleasableRunnable implements Runnable {

    private final EventBus m_eventBus;

    public ReleasableRunnable(EventBus m_eventBus) {
      this.m_eventBus = m_eventBus;
    }

    @Override
    public final void run() {
      List<T> events = retrieveBuffer();
      if (events.isEmpty()) {
        return;
      }
      released.set(false);
      mergeBufferAndPost(events, m_eventBus);
      released.set(true);
    }
  }
}
