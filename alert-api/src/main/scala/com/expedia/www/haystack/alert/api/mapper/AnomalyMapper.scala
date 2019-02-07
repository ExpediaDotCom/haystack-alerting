/*
 *               Copyright 2018 Expedia, Inc.
 *
 *        Licensed under the Apache License, Version 2.0 (the "License");
 *        you may not use this file except in compliance with the License.
 *        You may obtain a copy of the License at
 *
 *            http://www.apache.org/licenses/LICENSE-2.0
 *
 *        Unless required by applicable law or agreed to in writing, software
 *        distributed under the License is distributed on an "AS IS" BASIS,
 *        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *        See the License for the specific language governing permissions and
 *        limitations under the License.
 *
 */

package com.expedia.www.haystack.alert.api.mapper

import com.expedia.open.tracing.api.anomaly._
import com.expedia.www.anomaly.store.backend.api.AnomalyWithId
import com.expedia.www.haystack.alerting.commons.AnomalyTagKeys

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Try

object AnomalyMapper {

  private val FALLBACK_VALUE = -1.0

  def getSearchAnomaliesResponse(results: List[Try[Seq[AnomalyWithId]]]): SearchAnomaliesResponse = {
    val searchAnomalyResponses: List[SearchAnamolyResponse] = results.flatMap(result => result.get)
      .groupBy(anomaly => getLabelsAsString(anomaly.anomaly.tags.asScala.toMap)).map(
      {
        case (key, value) => {
          val anomalies = value.map(data => {
            val expectedValue = parseValue(data.anomaly.tags.get(AnomalyTagKeys.EXPECTED_VALUE))
            val observedValue = parseValue(data.anomaly.tags.get(AnomalyTagKeys.OBSERVED_VALUE))
            Anomaly.newBuilder().setTimestamp(data.anomaly.timestamp)
              .setExpectedValue(expectedValue).setObservedValue(observedValue).build()
          })
          SearchAnamolyResponse.newBuilder().addAllAnomalies(anomalies.asJava).setName(key).putAllLabels(value.head.anomaly.tags).build()
        }
      }).toList
    SearchAnomaliesResponse.newBuilder().addAllSearchAnomalyResponse(searchAnomalyResponses.asJava).build()
  }


  private def getLabelsAsString(label: Map[String, String]): String = {
    ListMap(label.toSeq.sortBy(_._1): _*).map(tuple => s"${tuple._1}=${tuple._2}").mkString(",")
  }

  private def parseValue(value: String): Double = {
    Try(value.toDouble).toOption match {
      case Some(v) => v
      case None => FALLBACK_VALUE
    }
  }
}
