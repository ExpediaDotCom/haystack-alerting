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

import java.util.Properties
import java.util.concurrent.Executors

import com.expedia.metrics.jackson.MetricsJavaModule
import com.expedia.metrics.{MetricData, MetricDefinition, TagCollection}
import com.expedia.www.anomaly.store.scalatest.IntegrationSuite
import com.expedia.www.anomaly.store.serde.AnomalyResult
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.HttpHost
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.elasticsearch.index.query.{QueryBuilders, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.junit.Assert
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters._

@IntegrationSuite
class AppIntegrationSpec extends FunSpec with Matchers {

  private val KAFKA_TOPIC = "anomalies"
  private val SERVICE_TAG_KEY = "service"
  private val OPERATION_TAG_KEY = "operation"
  private val currentTimeSec = System.currentTimeMillis / 1000

  private val mapper = new ObjectMapper()
    .registerModule(new DefaultScalaModule)
    .registerModule(new MetricsJavaModule)

  describe("Anomaly Store App") {
    it("should start the app and store the anomalies from kafka to elasticsearch") {
      Executors.newSingleThreadExecutor.execute(() => {
        try
          App.main(Array[String]("integration-test.conf"))
        catch {
          case e: Exception =>
            Assert.fail("Fail to start the app with error message " + e.getMessage)
        }
      })

      // sleep for enough to let app start
      Thread.sleep(40000)
      produceAnomaliesInKakfa()

      // sleep for kafka to flush
      Thread.sleep(5000)
      verifyElasticSearchData()
    }
  }

  private def verifyElasticSearchData() = {
    val client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://elasticsearch:9200")))
    for (svc <- List("svc1", "svc2")) {
      val sourceBuilder = new SearchSourceBuilder
      val boolQuery = QueryBuilders.boolQuery.must(QueryBuilders.matchQuery("tags." + SERVICE_TAG_KEY, svc))
      boolQuery.must(new RangeQueryBuilder("startTime").gte(currentTimeSec - 15).lte(currentTimeSec))
      sourceBuilder.query(boolQuery)
      val searchRequest = new SearchRequest().source(sourceBuilder).indices("haystack-anomalies*")
      val response = client.search(searchRequest)
      Assert.assertEquals(response.getHits.getHits.length, 1)
      val anomaly = response.getHits.getAt(0).getSourceAsMap
      Assert.assertEquals(anomaly.get("tags").asInstanceOf[java.util.Map[String, String]].get(SERVICE_TAG_KEY), svc)
    }
  }

  private def produceAnomaliesInKakfa() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafkasvc:9092")
    val producer = new KafkaProducer[String, Array[Byte]](props, new StringSerializer, new ByteArraySerializer)
    val a1 = createAnomalyJson("svc1")
    val a2 = createAnomalyJson("svc2")
    List(a1, a2) foreach { a =>
      producer.send(new ProducerRecord[String, Array[Byte]](KAFKA_TOPIC, "k1", a), (metadata: RecordMetadata, exception: Exception) => {
        if (exception != null) Assert.fail("Fail to produce the message to kafka with error message " + exception.getMessage)
      })
    }
    producer.flush()
  }

  private def createAnomalyJson(serviceName: String): Array[Byte] = {
    val tags = Map(SERVICE_TAG_KEY -> serviceName, OPERATION_TAG_KEY -> "/foo", MetricDefinition.MTYPE -> "gauge", MetricDefinition.UNIT -> "microseconds").asJava
    val tagCollection = new TagCollection(tags)
    val metricDef = new MetricDefinition("duration", tagCollection, TagCollection.EMPTY)
    val metricData = new MetricData(metricDef, 100.2, currentTimeSec)
    mapper.writeValueAsBytes(AnomalyResult(metricData))
  }
}
