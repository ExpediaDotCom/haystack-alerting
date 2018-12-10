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

import com.expedia.www.anomaly.store.config.AppConfiguration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.{FunSpec, Matchers}

class AppConfigurationSpec extends FunSpec with Matchers {
    describe("App Configuration") {
        it("should load the configuration") {
            val appConfig = new AppConfiguration("test.conf")
            val kafka = appConfig.kafkaConfig
            kafka.threads shouldBe 2
            kafka.parallelWrites shouldBe 10
            kafka.closeTimeoutMillis shouldBe 5000
            kafka.maxCommitRetries shouldBe 10
            kafka.commitBackOffMillis shouldBe 200
            kafka.commitIntervalMillis shouldBe 2000

            kafka.pollTimeoutMillis shouldBe 2000
            kafka.maxWakeups shouldBe 10
            kafka.wakeupTimeoutInMillis shouldBe 5000
            kafka.topic shouldEqual "anomalies"

            kafka.consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
            kafka.consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG) shouldEqual "haystack-anomaly-store"
            kafka.consumerConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) shouldEqual "latest"
            kafka.consumerConfig.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) shouldEqual "false"

            appConfig.healthStatusFilePath shouldEqual "/tmp/health.status"

            val pluginConfig = appConfig.pluginConfig
            pluginConfig.directory shouldEqual "storage-backends/elasticsearch/target"
            pluginConfig.jarName shouldEqual "elasticsearch-store-1.0.0-SNAPSHOT.jar"
            pluginConfig.name shouldEqual "elasticsearch"
            pluginConfig.conf.entrySet().toString shouldEqual "[host=Quoted(\"http://localhost:9200\")]"
        }
    }
}
