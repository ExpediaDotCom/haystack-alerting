/*
 * Copyright 2018 Expedia Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.expedia.www.anomaly.store

import java.io.{Closeable, IOException}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.www.anomaly.store.backend.api.AnomalyStore
import com.expedia.www.anomaly.store.config.KafkaConfig
import com.expedia.www.anomaly.store.task.{StoreTask, TaskStateListener}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class AnomalyStoreController private[store](config: KafkaConfig,
                                            store: AnomalyStore,
                                            healthController: HealthController) extends TaskStateListener with Closeable {
  private val LOGGER = LoggerFactory.getLogger(classOf[AnomalyStoreController])

  private val streamThreadExecutor = Executors.newFixedThreadPool(config.threads)
  private val isStarted = new AtomicBoolean(false)
  private val tasks = mutable.ListBuffer[StoreTask]()

  @throws[InterruptedException]
  @throws[IOException]
  private[store] def start() = {
    LOGGER.info("Starting the span indexing stream..")
    val parallelWritesPerTask = Math.ceil(config.parallelWrites / config.threads).toInt
    0 until config.threads foreach { taskId =>
      val task = new StoreTask(taskId, config, store, parallelWritesPerTask)
      task.setStateListener(this)
      tasks += task
      streamThreadExecutor.execute(task)
    }
    isStarted.set(true)
    healthController.setHealthy()
  }

  override def onChange(state: TaskStateListener.State.Value): Unit = {
    if (state == TaskStateListener.State.FAILED) {
      LOGGER.error("Thread state has changed to 'FAILED'")
      healthController.setUnhealthy()
    }
    else {
      LOGGER.info("Task state has changed to {}", state)
    }
  }

  override def close(): Unit = {
    if (isStarted.getAndSet(false)) {
      val shutdownThread = new Thread(() => {
        tasks.foreach(task => task.close())
      })
      shutdownThread.setDaemon(true)
      shutdownThread.run()
    }
  }
}
