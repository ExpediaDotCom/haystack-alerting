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

package com.expedia.www.anomaly.store.task

import java.io.Closeable
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.www.anomaly.store.backend.api.{Anomaly, AnomalyStore, AnomalyWithId}
import com.expedia.www.anomaly.store.config.KafkaConfig
import com.expedia.www.anomaly.store.serde.AnomalyDeserializer
import com.expedia.www.haystack.alerting.commons.AnomalyTagKeys
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
object StoreTask {
  private val LOGGER = LoggerFactory.getLogger(classOf[StoreTask])
  private val FILTER_TAG_KEY = "product"
  private val FILTER_TAG_VALUE = "haystack"
  private val STRONG_ANOMALY_LEVEL = "strong"
  private val WEAK_ANOMALY_LEVEL = "weak"

  private def createConsumer(taskId: Integer, cfg: KafkaConfig): KafkaConsumer[String, Anomaly] = {
    val props = cfg.consumerConfig
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, taskId.toString)
    new KafkaConsumer[String, Anomaly](props, new StringDeserializer, new AnomalyDeserializer)
  }
}

class StoreTask(taskId: Int, cfg: KafkaConfig, store: AnomalyStore, parallelWrites: Int) extends Runnable with Closeable {
  import StoreTask._

  private[task] val stateListeners = mutable.ListBuffer[TaskStateListener]()

  private val wakeupScheduler = Executors.newScheduledThreadPool(1)
  private val shutdownRequested = new AtomicBoolean(false)

  private var state = TaskStateListener.State.NOT_RUNNING
  private var lastCommitTime = System.currentTimeMillis

  private val callbacks = new java.util.LinkedList[StoreWriteCallback]()
  private val parallelWritesSemaphore = new Semaphore(parallelWrites)
  private val consumer: KafkaConsumer[String, Anomaly] = createConsumer(taskId, cfg)
  private var wakeups = 0

  // subscribe to the consumer topic
  consumer.subscribe(java.util.Collections.singletonList(cfg.topic), new RebalanceListener)

  private class RebalanceListener extends ConsumerRebalanceListener {
    /**
      * close the running processors for the revoked partitions
      *
      * @param revokedPartitions revoked partitions
      */
    override def onPartitionsRevoked(revokedPartitions: java.util.Collection[TopicPartition]): Unit = {
      LOGGER.info("Partitions {} revoked at the beginning of consumer rebalance for taskId={}", revokedPartitions, taskId)
    }

    /**
      * create processors for newly assigned partitions
      *
      * @param assignedPartitions newly assigned partitions
      */
    override def onPartitionsAssigned(assignedPartitions: java.util.Collection[TopicPartition]): Unit = {
      LOGGER.info("Partitions {} assigned at the beginning of consumer rebalance for taskId={}", assignedPartitions, taskId)
    }
  }

  override def run(): Unit = {
    LOGGER.info("Starting stream processing thread with id={}", taskId)
    try {
      updateStateAndNotify(TaskStateListener.State.RUNNING)
      runLoop()
    } catch {
      case ie: InterruptedException =>
        LOGGER.error("This stream task with taskId={} has been interrupted", taskId, ie)
      case ex: Exception =>
        if (!shutdownRequested.get) updateStateAndNotify(TaskStateListener.State.FAILED)
        // may be logging the exception again for kafka specific exceptions, but it is ok.
        LOGGER.error("Stream application faced an exception during processing for taskId={}: ", taskId, ex)
    } finally {
      close()
      updateStateAndNotify(TaskStateListener.State.CLOSED)
    }
  }

  override def close(): Unit = {
    shutdownRequested.set(true)
    consumer.close(cfg.closeTimeoutMillis.toLong, TimeUnit.MILLISECONDS)
    wakeupScheduler.shutdown()
  }

  def setStateListener(listener: TaskStateListener): Unit = {
    this.stateListeners += listener
  }

  private def findCommittableOffset(): mutable.Map[TopicPartition, OffsetAndMetadata] = {
    var committableOffsets = mutable.Map[TopicPartition, OffsetAndMetadata]()
    while (true) {
      val cbk = callbacks.peek()
      if (cbk != null && cbk.callbackReceived) {
        // remove the completed callback from the queue
        committableOffsets = callbacks.poll().offsets
      } else {
        return committableOffsets
      }
    }
    committableOffsets
  }

  /**
    * run the consumer loop till the shutdown is requested or any exception is thrown
    */
  @throws[InterruptedException]
  private def runLoop() = {
    while (!shutdownRequested.get) {
      poll() match {
        case Some(records) =>
          val offsets = mutable.Map[TopicPartition, OffsetAndMetadata]()
          val saveAnomalies = mutable.ListBuffer[AnomalyWithId]()

          for (record <- records.asScala) {
            val anomalyWithId = transform(record)
            if (anomalyWithId.anomaly.tags.containsKey(FILTER_TAG_KEY)
                && anomalyWithId.anomaly.tags.get(FILTER_TAG_KEY).equalsIgnoreCase(FILTER_TAG_VALUE)
                && (STRONG_ANOMALY_LEVEL.equalsIgnoreCase(anomalyWithId.anomaly.tags.get(AnomalyTagKeys.ANOMALY_LEVEL)) ||
                      WEAK_ANOMALY_LEVEL.equalsIgnoreCase(anomalyWithId.anomaly.tags.get(AnomalyTagKeys.ANOMALY_LEVEL)))
            ) {
              saveAnomalies += anomalyWithId
            }
            updateOffset(offsets, record)
          }

          // see if the execution is permitted as per parallel writes semaphore
          parallelWritesSemaphore.acquire()

          // find the offset that needs to be committed
          val committableOffsets = findCommittableOffset()

          val callback = new StoreWriteCallback(offsets)
          callbacks.push(callback)
          if(saveAnomalies.nonEmpty) {
            store.write(saveAnomalies, callback)
          } else {
            callback.onComplete(null)
          }

          // commit offsets
          commit(committableOffsets.asJava, 0)

        case _ =>
      }
    }
  }

  private class StoreWriteCallback private[task](val offsets: mutable.Map[TopicPartition, OffsetAndMetadata]) extends AnomalyStore.WriteCallback {
    @volatile
    var callbackReceived = false

    def onComplete(ex: Exception): Unit = {
      if (ex == null) {
        callbackReceived = true
        parallelWritesSemaphore.release()
      }
      else { // dont commit anything if exception happens
        LOGGER.error("Fail to write to elastic search after all retries with error", ex)
        updateStateAndNotify(TaskStateListener.State.FAILED)
      }
    }
  }

  private def updateOffset(committableOffsets: mutable.Map[TopicPartition, OffsetAndMetadata], record: ConsumerRecord[String, Anomaly]): Unit = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    committableOffsets.get(topicPartition) match {
      case Some(offset) => if (offset.offset < record.offset) {
        committableOffsets.put(topicPartition, new OffsetAndMetadata(record.offset))
      }
      case _ => committableOffsets.put(topicPartition, new OffsetAndMetadata(record.offset))
    }
  }

  private def transform(record: ConsumerRecord[String, Anomaly]): AnomalyWithId = {
    val id = record.topic + "-" + record.partition + "-" + record.offset
    AnomalyWithId(id, record.value())
  }

  private def updateStateAndNotify(newState: TaskStateListener.State.Value) = {
    if (state != newState) {
      state = newState
      // invoke listeners for any state change
      stateListeners.foreach((listener) => listener.onChange(state))
    }
  }

  /**
    * before requesting consumer.poll(), schedule a wakeup call as poll() may hang due to network errors in kafka
    * if the poll() doesnt return after a timeout, then wakeup the consumer.
    *
    * @return consumer records from kafka
    */
  private def poll(): Option[ConsumerRecords[String, Anomaly]] = {
    val wakeupCall = scheduleWakeup
    try {
      val records = consumer.poll(cfg.pollTimeoutMillis)
      wakeups = 0
      if(records.isEmpty) None else Some(records)
    } catch {
      case we: WakeupException =>
        handleWakeupError(we)
        None
    } finally {
      try {
        wakeupCall.cancel(true)
      } catch {
        case ex: Exception => LOGGER.error("kafka consumer poll has failed with error", ex)
      }
    }
  }

  /**
    * commit the offset to kafka with a retry logic
    *
    * @param offsets      map of offsets for each topic partition
    * @param retryAttempt current retry attempt
    */
  @throws[InterruptedException]
  @tailrec
  private def commit(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], retryAttempt: Int): Unit = {
    try {
      val currentTime = System.currentTimeMillis
      if (currentTime - lastCommitTime >= cfg.commitIntervalMillis && !offsets.isEmpty && retryAttempt <= cfg.maxCommitRetries) {
        LOGGER.info("committing the offsets now for taskId {}", taskId)
        consumer.commitSync(offsets)
        lastCommitTime = currentTime
      }
    } catch {
      case cfe: CommitFailedException =>
        LOGGER.error("Fail to commit offset to kafka with error", cfe)
        Thread.sleep(cfg.commitBackOffMillis)
        // retry offset again
        commit(offsets, retryAttempt + 1)
      case ex: Exception =>
        LOGGER.error("Fail to commit the offsets with exception", ex)
    }
  }

  private def scheduleWakeup = wakeupScheduler.schedule(new Runnable {
    override def run(): Unit = {
      consumer.wakeup()
    }
  }, cfg.wakeupTimeoutInMillis, TimeUnit.MILLISECONDS)

  private def handleWakeupError(we: WakeupException): Unit = {
    if (we == null) return
    // if in shutdown phase, then do not swallow the exception, throw it to upstream
    if (shutdownRequested.get) throw we
    wakeups = wakeups + 1
    if (wakeups == cfg.maxWakeups) {
      LOGGER.error("WakeupException limit exceeded, throwing up wakeup exception for taskId={}.", taskId, we)
      throw we
    }
    else {
      LOGGER.error(s"Consumer poll took more than ${cfg.wakeupTimeoutInMillis} ms for taskId=$taskId, wakeup attempt=$wakeups!. Will try poll again!")
    }
  }
}
