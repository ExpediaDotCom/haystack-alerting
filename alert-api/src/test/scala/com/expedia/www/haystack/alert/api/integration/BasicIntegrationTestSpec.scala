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

package com.expedia.www.haystack.alert.api.integration

import java.time.Instant
import java.util.concurrent.Executors

import com.expedia.open.tracing.api.anomaly.AnomalyReaderGrpc.AnomalyReaderBlockingStub
import com.expedia.open.tracing.api.anomaly.{AnomalyReaderGrpc, SearchAnamoliesRequest}
import com.expedia.open.tracing.api.subscription.SubscriptionManagementGrpc._
import com.expedia.open.tracing.api.subscription._
import com.expedia.www.anomaly.store.backend.api.{Anomaly, AnomalyWithId}
import com.expedia.www.haystack.alert.api.{App, IntegrationSuite}
import io.grpc.ManagedChannelBuilder
import io.grpc.health.v1.HealthGrpc
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

@IntegrationSuite
trait BasicIntegrationTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  protected var client: SubscriptionManagementBlockingStub = _
  protected var anomalyClient: AnomalyReaderBlockingStub = _
  protected var healthCheckClient: HealthGrpc.HealthBlockingStub = _
  private val executors = Executors.newSingleThreadExecutor()

  override def beforeAll(): Unit = {

    executors.submit(new Runnable {
      override def run(): Unit = {
        App.main(Array[String]())
      }
    })

    Thread.sleep(10000)

    val promise = Promise[Unit]()
    App.stores.head.write(getAnomalies, (ex: Exception) => {
      if (ex != null) {
        print("Failed in setting up the test")
        promise.failure(ex)
      }else {
        promise.success()
      }
    })

    client = SubscriptionManagementGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8088)
      .usePlaintext(true).build())

    anomalyClient = AnomalyReaderGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8088)
      .usePlaintext(true).build())

    healthCheckClient = HealthGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8088)
      .usePlaintext(true)
      .build())

    Await.result(promise.future, 5 seconds)
  }


  protected def getCreateSubscriptionRequest(): CreateSubscriptionRequest = {
    val subscriptionRequest: SubscriptionRequest = getSubscriptionRequest
    CreateSubscriptionRequest.newBuilder().setUser(User.newBuilder().setUsername("haystack"))
      .setSubscriptionRequest(subscriptionRequest).build()
  }


  protected def getUpdateSubscriptionRequest: UpdateSubscriptionRequest = {
    val subscriptionRequest: SubscriptionRequest = getSubscriptionRequest
    UpdateSubscriptionRequest.newBuilder().setSubscriptionId("1")
      .setSubscriptionRequest(subscriptionRequest).build()
  }

  protected def getGetSubscriptionRequest: GetSubscriptionRequest = {
    GetSubscriptionRequest.newBuilder().setSubscriptionId("1").build()
  }

  protected def getDeleteSubscriptionRequest: DeleteSubscriptionRequest = {
    DeleteSubscriptionRequest.newBuilder().setSubscriptionId("1").build()
  }

  protected def getSearchSubscriptionRequest: SearchSubscriptionRequest = {
    val labels = Map("servicename" -> "abc", "interval" -> "1m").asJava
    SearchSubscriptionRequest.newBuilder().putAllLabels(labels).build()
  }


  private def getSubscriptionRequest = {
    val field1 = Field.newBuilder().setName("product").setValue("haystack").build()
    val field2 = Field.newBuilder().setName("serviceName").setValue("abc").build()
    val operand1 = Operand.newBuilder().setField(field1).build()
    val operand2 = Operand.newBuilder().setField(field2).build()

    val expressionTree = ExpressionTree.newBuilder().setOperator(ExpressionTree.Operator.AND)
      .addAllOperands(List(operand1, operand2).asJava).build()
    val dispatcher = Dispatcher.newBuilder().setEndpoint("haystack").setType(DispatchType.SLACK).build()

    val subscriptionRequest = SubscriptionRequest.newBuilder().addAllDispatchers(List(dispatcher).asJava)
      .setExpressionTree(expressionTree).build()
    subscriptionRequest
  }

  protected def getAnomalies: List[AnomalyWithId] = {
    val currentTimestamp = Instant.now().toEpochMilli
    val labels1 = Map("product" -> "haystack", "servicename" -> "def").asJava
    val labels2 = Map("product" -> "haystack", "servicename" -> "abc").asJava
    List(AnomalyWithId("1", new Anomaly(labels1, currentTimestamp)), AnomalyWithId("2", new Anomaly(labels2, currentTimestamp)))
  }

  protected def searchAnomaliesRequest(requestLabels : Map[String, String]): SearchAnamoliesRequest = {
    SearchAnamoliesRequest.newBuilder().putAllLabels(requestLabels.asJava)
      .setStartTime(1).setEndTime(Instant.now().toEpochMilli).setSize(-1).build()
  }



}
