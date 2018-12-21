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

import java.util.concurrent.Executors

import com.expedia.open.tracing.api.subscription._
import com.expedia.open.tracing.api.subscription.SubscriptionManagementGrpc._
import com.expedia.www.haystack.alert.api.{App, IntegrationSuite}
import io.grpc.ManagedChannelBuilder
import io.grpc.health.v1.HealthGrpc
import org.scalatest._
import scala.collection.JavaConverters._

@IntegrationSuite
trait BasicIntegrationTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  protected var client: SubscriptionManagementBlockingStub = _
  protected var healthCheckClient: HealthGrpc.HealthBlockingStub = _
  private val executors = Executors.newSingleThreadExecutor()

  override def beforeAll(): Unit = {

    executors.submit(new Runnable {
      override def run(): Unit = App.main(Array[String]())
    })

    Thread.sleep(5000)

    client = SubscriptionManagementGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8088)
      .usePlaintext(true).build())

    healthCheckClient = HealthGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8088)
      .usePlaintext(true)
      .build())

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
}
