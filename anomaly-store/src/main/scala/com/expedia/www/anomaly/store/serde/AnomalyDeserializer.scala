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

package com.expedia.www.anomaly.store.serde

import java.util

import com.expedia.adaptivealerting.core.data.MappedMetricData
import com.expedia.metrics.jackson.MetricsJavaModule
import com.expedia.www.anomaly.store.backend.api.Anomaly
import com.expedia.www.haystack.alerting.commons.AnomalyTagKeys
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Deserializer


class AnomalyDeserializer extends Deserializer[Anomaly] {
  private val mapper = new ObjectMapper()
    .registerModule(new DefaultScalaModule)
    .registerModule(new MetricsJavaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(s: String, bytes: Array[Byte]): Anomaly = {
    if (bytes == null || bytes.isEmpty) return null

    val result = mapper.readValue(bytes, classOf[MappedMetricData])
    val tags = new util.HashMap[String, String](result.getMetricData.getMetricDefinition.getTags.getKv)
    val expectedValue = result.getAnomalyResult.getPredicted
    val anomalyLevel = result.getAnomalyResult.getAnomalyLevel
    val observedValue = result.getMetricData.getValue

    tags.put(AnomalyTagKeys.METRIC_KEY, result.getMetricData.getMetricDefinition.getKey)
    tags.put(AnomalyTagKeys.EXPECTED_VALUE, expectedValue.toString)
    tags.put(AnomalyTagKeys.OBSERVED_VALUE, observedValue.toString)
    tags.put(AnomalyTagKeys.ANOMALY_LEVEL, anomalyLevel.toString)
    // timestamp is in seconds
    Anomaly(tags, result.getMetricData.getTimestamp)
  }
}
