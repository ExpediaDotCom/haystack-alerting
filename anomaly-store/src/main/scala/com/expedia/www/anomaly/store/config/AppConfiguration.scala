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

package com.expedia.www.anomaly.store.config

import java.util.Properties

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._
/**
 * This class reads the configuration from the given resource name using ConfigurationLoader
 *
 * @param resourceName name of the resource file to load
 */
class AppConfiguration(resourceName: String) {
  require(resourceName != null && !resourceName.isEmpty)

  private val config = ConfigurationLoader.loadConfigFileWithEnvOverrides(resourceName = this.resourceName)

  /**
    * default constructor. Loads config from resource name to "app.conf"
    *
    * */
  def this() = this("base.conf")

  val healthStatusFilePath: String = config.getString("health.status.path")

  lazy val kafkaConfig: KafkaConfig = {

    // verify if the applicationId and bootstrap server config are non empty
    def verifyRequiredProps(props: Properties): Unit = {
      require(props.getProperty(ConsumerConfig.GROUP_ID_CONFIG) != null)
      require(props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) != null)
    }

    def addProps(config: Config): Properties = {
      val props = new Properties()
      config.entrySet().asScala.foreach(kv => {
        props.setProperty(kv.getKey, kv.getValue.unwrapped().toString)
      })

      verifyRequiredProps(props)
      props
    }

    val kafka = config.getConfig("kafka")
    KafkaConfig(
      threads = kafka.getInt("threads"),
      topic = kafka.getString("topic"),
      maxWakeups = kafka.getInt("wakeup.max"),
      wakeupTimeoutInMillis = kafka.getInt("wakeup.timeout.ms"),
      maxCommitRetries = kafka.getInt("commit.retries"),
      commitIntervalMillis = kafka.getInt("commit.interval.ms"),
      commitBackOffMillis = kafka.getInt("commit.backoff.ms"),
      closeTimeoutMillis = kafka.getInt("close.timeout.ms"),
      pollTimeoutMillis = kafka.getInt("poll.timeout.ms"),
      parallelWrites = kafka.getInt("parallel.writes"),
      consumerConfig = addProps(kafka.getConfig("consumer")))
  }

  lazy val pluginConfig: PluginConfig = {
    val plugin = config.getConfig("plugin")
    PluginConfig(
      plugin.getString("directory"),
      plugin.getString("name"),
      plugin.getString("jar.name"),
      plugin.getConfig("conf"))
  }
}