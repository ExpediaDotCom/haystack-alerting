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

package com.expedia.www.haystack.alert.api.unit.manager.anomaly

import java.time.Instant

import com.expedia.open.tracing.api.anomaly.SearchAnamoliesRequest
import com.expedia.www.anomaly.store.backend.api.{Anomaly, AnomalyStore, AnomalyWithId}
import com.expedia.www.haystack.alert.api.manager.AnomalyReaderManager
import com.expedia.www.haystack.alert.api.unit.BasicUnitTestSpec
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class AnomalyReaderSpec extends BasicUnitTestSpec {

  private val requestLabels = Map("product" -> "haystack")
  private val currentTimestamp = Instant.now().toEpochMilli

  "Anomaly Reader" should {

    "should coalesce and return same anomalies given a valid search anomalies request" in {

      Given("AnomalyReader and search anomalies request")
      val labels = Map("product" -> "haystack", "servicename" -> "abc")
      val successfulCallback1 = successfulCallBack(labels, currentTimestamp -100)
      val successfulCallback2 = successfulCallBack(labels, currentTimestamp)
      val anomalyStore = mockAnomalyStore(successfulCallback1, successfulCallback2)
      val anomalyReader = new AnomalyReaderManager(List(anomalyStore, anomalyStore))

      When("fetch anomalies is called")
      val result = anomalyReader.getAnomalies(searchAnamoliesRequest)


      Then("Coalesced Anomalies are returned from all the stores")
      val searchAnomaliesResponse = Await.result(result, 5 seconds)
      searchAnomaliesResponse.getSearchAnomalyResponseCount shouldBe 1
      val searchAnomalyResponse = searchAnomaliesResponse.getSearchAnomalyResponse(0)
      searchAnomalyResponse.getAnomaliesCount shouldBe 2
      searchAnomalyResponse.getLabelsMap shouldBe labels.asJava
      searchAnomalyResponse.getLabelsMap.entrySet().containsAll(requestLabels.asJava.entrySet()) shouldBe true
      searchAnomalyResponse.getAnomalies(0).getTimestamp shouldBe currentTimestamp - 100
      searchAnomalyResponse.getAnomalies(1).getTimestamp shouldBe currentTimestamp
      verify(anomalyStore, times(2)).read(ArgumentMatchers.eq(requestLabels), anyLong(), anyLong(), anyInt(), any(classOf[AnomalyStore.ReadCallback]))


    }


    "should return different anomalies given a valid search anomalies request" in {

      Given("AnomalyReader and search anomalies request")
      val successfulCallback1 = successfulCallBack(Map("product" -> "haystack", "servicename" -> "abc"), currentTimestamp -100)
      val successfulCallback2 = successfulCallBack(Map("product" -> "haystack", "servicename" -> "def"), currentTimestamp)
      val anomalyStore = mockAnomalyStore(successfulCallback1, successfulCallback2)
      val anomalyReader = new AnomalyReaderManager(List(anomalyStore, anomalyStore))

      When("fetch anomalies is called")
      val result = anomalyReader.getAnomalies(searchAnamoliesRequest)

      Then("Different Anomalies are returned from all the stores")
      val searchAnomaliesResponse = Await.result(result, 5 seconds)
      searchAnomaliesResponse.getSearchAnomalyResponseCount shouldBe 2
      val searchAnomalyResponse = searchAnomaliesResponse.getSearchAnomalyResponse(0)
      searchAnomalyResponse.getAnomaliesCount shouldBe 1
      searchAnomalyResponse.getLabelsMap.entrySet().containsAll(requestLabels.asJava.entrySet()) shouldBe true
      verify(anomalyStore, times(2)).read(ArgumentMatchers.eq(requestLabels), anyLong(), anyLong(), anyInt(), any(classOf[AnomalyStore.ReadCallback]))

    }


    "should return different and coalesced anomalies given a valid search anomalies request" in {

      Given("AnomalyReader and search anomalies request")
      val successfulCallback1 = successfulCallBack(Map("product" -> "haystack", "servicename" -> "abc"), currentTimestamp -100)
      val successfulCallback2 = new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val labels1 = Map("product" -> "haystack", "servicename" -> "def").asJava
          val labels2 = Map("product" -> "haystack", "servicename" -> "abc").asJava
          val successfulAnomalies = List(AnomalyWithId("1", new Anomaly(labels1, currentTimestamp)), AnomalyWithId("2", new Anomaly(labels2, currentTimestamp)))
          invocation.getArgument[AnomalyStore.ReadCallback](4).onComplete(successfulAnomalies, null)
        }
      }
      val anomalyStore = mockAnomalyStore(successfulCallback1, successfulCallback2)
      val anomalyReader = new AnomalyReaderManager(List(anomalyStore, anomalyStore))


      When("fetch anomalies is called")
      val result = anomalyReader.getAnomalies(searchAnamoliesRequest)


      Then("Anomalies are coalesced and different anomalies are returned from all the stores")
      val searchAnomaliesResponse = Await.result(result, 5 seconds)
      searchAnomaliesResponse.getSearchAnomalyResponseCount shouldBe 2
      var searchAnomalyResponse = searchAnomaliesResponse.getSearchAnomalyResponse(0)
      if(searchAnomalyResponse.getAnomaliesCount == 1) {
        searchAnomalyResponse.getLabelsMap.asScala("servicename") shouldBe "def"
      } else {
        searchAnomalyResponse.getLabelsMap.asScala("servicename") shouldBe "abc"
      }
      searchAnomalyResponse.getLabelsMap.entrySet().containsAll(requestLabels.asJava.entrySet()) shouldBe true
      searchAnomalyResponse = searchAnomaliesResponse.getSearchAnomalyResponse(1)
      if(searchAnomalyResponse.getAnomaliesCount == 1) {
        searchAnomalyResponse.getLabelsMap.asScala("servicename") shouldBe "def"
      } else {
        searchAnomalyResponse.getLabelsMap.asScala("servicename") shouldBe "abc"
      }
      verify(anomalyStore, times(2)).read(ArgumentMatchers.eq(requestLabels), anyLong(), anyLong(), anyInt(), any(classOf[AnomalyStore.ReadCallback]))


    }


    "should return partial anomalies given a valid search anomalies request in case an exception from one of the store" in {

      Given("AnomalyReader and search anomalies request")
      val responseLabels = Map("product" -> "haystack", "servicename" -> "abc")
      val anomalyStore = mockAnomalyStore(successfulCallBack(responseLabels, currentTimestamp - 100), failedCallBack)
      val anomalyReader = new AnomalyReaderManager(List(anomalyStore, anomalyStore))


      When("fetch anomalies is called")
      val result = anomalyReader.getAnomalies(searchAnamoliesRequest)


      Then("Partial Anomalies are returned from all the stores")
      val searchAnomaliesResponse = Await.result(result, 5 seconds)
      searchAnomaliesResponse.getSearchAnomalyResponseCount shouldBe 1
      val searchAnomalyResponse = searchAnomaliesResponse.getSearchAnomalyResponse(0)
      searchAnomalyResponse.getLabelsMap.asScala("servicename") shouldBe "abc"
      searchAnomalyResponse.getLabelsMap.entrySet().containsAll(requestLabels.asJava.entrySet()) shouldBe true
      verify(anomalyStore, times(2)).read(ArgumentMatchers.eq(requestLabels), anyLong(), anyLong(), anyInt(), any(classOf[AnomalyStore.ReadCallback]))


    }



    "should return no anomalies given a valid search anomalies request in case an exception from all stores" in {

      Given("AnomalyReader and search anomalies request")
      val anomalyStore = mockAnomalyStore(failedCallBack, failedCallBack)
      val anomalyReader = new AnomalyReaderManager(List(anomalyStore, anomalyStore))

      When("fetch anomalies is called")
      val result = anomalyReader.getAnomalies(searchAnamoliesRequest)

      Then("No Anomalies are returned from all the stores")
      val searchAnomaliesResponse = Await.result(result, 5 seconds)
      searchAnomaliesResponse.getSearchAnomalyResponseCount shouldBe 0
      verify(anomalyStore, times(2)).read(ArgumentMatchers.eq(requestLabels), anyLong(), anyLong(), anyInt(), any(classOf[AnomalyStore.ReadCallback]))

    }

  }

  private val searchAnamoliesRequest = {
    SearchAnamoliesRequest.newBuilder().putAllLabels(requestLabels.asJava)
      .setStartTime(1).setEndTime(Instant.now().toEpochMilli).setSize(-1).build()
  }

  private def successfulCallBack(labels: Map[String,String], timestamp: Long) = new Answer[Unit] {
    override def answer(invocation: InvocationOnMock): Unit = {
      val successfulAnomalies = List(AnomalyWithId("1", new Anomaly(labels.asJava, timestamp)))
      invocation.getArgument[AnomalyStore.ReadCallback](4).onComplete(successfulAnomalies, null)
    }
  }

  private val failedCallBack = new Answer[Unit] {
    override def answer(invocation: InvocationOnMock): Unit = {
      invocation.getArgument[AnomalyStore.ReadCallback](4).onComplete(null, new RuntimeException("Failed to call store"))
    }
  }


  private def mockAnomalyStore(answer1 : Answer[Unit], answer2 : Answer[Unit]) : AnomalyStore = {
    val anomalyStore = mock(classOf[AnomalyStore])
    when(anomalyStore.read(ArgumentMatchers.eq(requestLabels), anyLong(), anyLong(), anyInt(), any(classOf[AnomalyStore.ReadCallback]))).
      thenAnswer(answer1).thenAnswer(answer2)
    anomalyStore
  }

}
