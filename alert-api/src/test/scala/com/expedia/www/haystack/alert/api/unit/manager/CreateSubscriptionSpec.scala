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

package com.expedia.www.haystack.alert.api.unit.manager

import com.expedia.open.tracing.api.subscription._
import com.expedia.www.haystack.alert.api.client.AsyncHttpClient
import com.expedia.www.haystack.alert.api.manager.SubscriptionManager
import org.apache.http.HttpResponse
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

class CreateSubscriptionSpec extends SubscriptionManagerSpec {

  private val VALID_SUBSCRIPTION_RESULT = "[1]"


  "Subscription Manager " should {

    "should return subscription ids in case of valid create subscription request" in {

      Given("Subscription Manager and valid create subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)
      val createSubscriptionRequest = getValidCreateSubscriptionRequest


      When("create subscription request is called")
      val future = subscriptionManager.createSubscriptionRequest(createSubscriptionRequest)

      Then("subscription ids are returned")
      val createSubscriptionResponse = Await.result(future, 5 seconds)
      verify(asyncHttpClient, times(1)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))
      createSubscriptionResponse.getSubscriptionId shouldBe "1"


    }


    "should throw an exception in case of invalid create subscription request" in {

      Given("Subscription Manager and invalid create subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(ERROR_SUBSCRIPTION_RESULT, ERROR_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)
      val createSubscriptionRequest = getInvalidCreateSubscriptionRequest


      When("create subscription request is called")
      val future = subscriptionManager.createSubscriptionRequest(createSubscriptionRequest)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception should have message s"Failed with status code ${ERROR_STATUS_CODE} with error ${ERROR_SUBSCRIPTION_RESULT}"
      verify(asyncHttpClient, times(1)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))


    }


    "should retry in case of exception and create subscription in case of valid create subscription request" in {

      Given("Subscription Manager and valid create subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      val successPromise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        failurePromise.future).thenReturn(successPromise.future)
      successPromise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)
      val createSubscriptionRequest = getValidCreateSubscriptionRequest


      When("create subscription request is called")
      val future = subscriptionManager.createSubscriptionRequest(createSubscriptionRequest)

      Then("subscription ids are returned after retrying")
      val exception = intercept[Exception] {
        Await.result(future, appConfiguration.subscriptionConfig.retryInSeconds - 1 seconds)
      }
      val createSubscriptionResponse = Await.result(future, 5 seconds)
      exception should have message s"Futures timed out after [${appConfiguration.subscriptionConfig.retryInSeconds - 1} seconds]"
      createSubscriptionResponse.getSubscriptionId shouldBe "1"
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))

    }


    "should retry in case of exception and throw an exception if all retries are exhausted" in {

      Given("Subscription Manager and invalid create subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        failurePromise.future)
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("create subscription request is called")
      val future = subscriptionManager.createSubscriptionRequest(getInvalidCreateSubscriptionRequest)

      Then("exception is thrown after exhausting all retries")
      val exception = intercept[Exception] {
        Await.result(future, (appConfiguration.subscriptionConfig.numOfRetries * appConfiguration.subscriptionConfig.retryInSeconds + 1) seconds)
      }
      exception should have message EXCEPTION
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries + 1)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))

    }

  }

  private def getValidCreateSubscriptionRequest: CreateSubscriptionRequest = {
    val field1 = Field.newBuilder().setName("product").setValue("haystack").build()
    val field2 = Field.newBuilder().setName("serviceName").setValue("abc").build()
    val operand1 = Operand.newBuilder().setField(field1).build()
    val operand2 = Operand.newBuilder().setField(field2).build()

    val expressionTree = ExpressionTree.newBuilder().setOperator(ExpressionTree.Operator.AND)
      .addAllOperands(List(operand1, operand2).asJava).build()
    val dispatcher = Dispatcher.newBuilder().setEndpoint("haystack").setType(DispatchType.SLACK).build()

    val subscriptionRequest = SubscriptionRequest.newBuilder().addAllDispatchers(List(dispatcher).asJava)
      .setExpressionTree(expressionTree).build()
    CreateSubscriptionRequest.newBuilder().setUser(User.newBuilder().setUsername("haystack"))
      .setSubscriptionRequest(subscriptionRequest).build()
  }


  private def getInvalidCreateSubscriptionRequest: CreateSubscriptionRequest = {
    val field1 = Field.newBuilder().setName("product").setValue("haystack").build()
    val field2 = Field.newBuilder().setName("serviceName").setValue("abc").build()
    val operand1 = Operand.newBuilder().setField(field1).build()
    val operand2 = Operand.newBuilder().setField(field2).build()

    val expressionTree = ExpressionTree.newBuilder().setOperator(ExpressionTree.Operator.AND)
      .addAllOperands(List(operand1, operand2).asJava).build()

    val subscriptionRequest = SubscriptionRequest.newBuilder().setExpressionTree(expressionTree).build()
    CreateSubscriptionRequest.newBuilder().setUser(User.newBuilder().setUsername("haystack"))
      .setSubscriptionRequest(subscriptionRequest).build()
  }


}
