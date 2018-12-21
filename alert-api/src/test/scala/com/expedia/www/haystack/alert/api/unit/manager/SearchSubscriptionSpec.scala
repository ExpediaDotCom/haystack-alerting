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

import com.expedia.alertmanager.model
import com.expedia.open.tracing.api.subscription._
import com.expedia.www.haystack.alert.api.client.AsyncHttpClient
import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.manager.SubscriptionManager
import org.apache.http.HttpResponse
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

class SearchSubscriptionSpec extends SubscriptionManagerSpec {

  private val VALID_SUBSCRIPTION_RESULT = new String(serde.serialize(getValidSearchSubscriptionResponse))


  "Subscription Manager " should {

    "should search subscriptions in case of valid search subscription request" in {

      Given("Subscription Manager and valid search subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("search subscription request is called")
      val future = subscriptionManager.searchSubscriptionRequest(getValidSearchSubscriptionRequest)

      Then("corresponding subscriptions are returned")
      val validSubscriptionResponses = Await.result(future, 5 seconds)
      verify(asyncHttpClient, times(1)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))
      validSubscriptionResponses should not be (null)
      val validSearchSubscriptionResponse = getValidSearchSubscriptionResponse
      validSubscriptionResponses.getSubscriptionResponseCount shouldBe validSearchSubscriptionResponse.length
      validSubscriptionResponses.getSubscriptionResponse(0).getSubscriptionId shouldBe validSearchSubscriptionResponse.head.getId
      validSubscriptionResponses.getSubscriptionResponse(0).getCreatedTime shouldBe validSearchSubscriptionResponse.head.getCreatedTime
      validSubscriptionResponses.getSubscriptionResponse(0).getLastModifiedTime shouldBe validSearchSubscriptionResponse.head.getLastModifiedTime
      validSubscriptionResponses.getSubscriptionResponse(0).getDispatchersList.asScala.length shouldBe validSearchSubscriptionResponse.head.getDispatchers.asScala.length
      validSubscriptionResponses.getSubscriptionResponse(0).getDispatchersList.asScala.head.getEndpoint shouldBe validSearchSubscriptionResponse.head.getDispatchers.asScala.head.getEndpoint
      validSubscriptionResponses.getSubscriptionResponse(0).getExpressionTree.getOperandsCount shouldBe validSearchSubscriptionResponse.head.getExpression.getOperands.asScala.length

    }


    "should throw an exception in case of invalid search subscription request" in {

      Given("Subscription Manager and search subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(ERROR_SUBSCRIPTION_RESULT, ERROR_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)

      When("search subscription request is called")
      val future = subscriptionManager.searchSubscriptionRequest(getInvalidSearchSubscriptionRequest)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception should have message s"Failed with status code ${ERROR_STATUS_CODE} with error ${ERROR_SUBSCRIPTION_RESULT}"
      verify(asyncHttpClient, times(1)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))


    }


    "should retry in case of exception and search subscription in case of search subscription request" in {

      Given("Subscription Manager and valid search subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      val successPromise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        failurePromise.future).thenReturn(successPromise.future)
      successPromise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("search subscription request is called")
      val future = subscriptionManager.searchSubscriptionRequest(getValidSearchSubscriptionRequest)

      Then("subscription results are returned after retrying")
      val exception = intercept[Exception] {
        Await.result(future, appConfiguration.subscriptionConfig.retryInSeconds - 1 seconds)
      }
      val validSubscriptionResponses = Await.result(future, 5 seconds)
      exception should have message s"Futures timed out after [${appConfiguration.subscriptionConfig.retryInSeconds - 1} seconds]"
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))
      validSubscriptionResponses should not be (null)
      val validSearchSubscriptionResponse = getValidSearchSubscriptionResponse
      validSubscriptionResponses.getSubscriptionResponseCount shouldBe validSearchSubscriptionResponse.length
      validSubscriptionResponses.getSubscriptionResponse(0).getSubscriptionId shouldBe validSearchSubscriptionResponse.head.getId
      validSubscriptionResponses.getSubscriptionResponse(0).getCreatedTime shouldBe validSearchSubscriptionResponse.head.getCreatedTime
      validSubscriptionResponses.getSubscriptionResponse(0).getLastModifiedTime shouldBe validSearchSubscriptionResponse.head.getLastModifiedTime
      validSubscriptionResponses.getSubscriptionResponse(0).getDispatchersList.asScala.length shouldBe validSearchSubscriptionResponse.head.getDispatchers.asScala.length
      validSubscriptionResponses.getSubscriptionResponse(0).getDispatchersList.asScala.head.getEndpoint shouldBe validSearchSubscriptionResponse.head.getDispatchers.asScala.head.getEndpoint
      validSubscriptionResponses.getSubscriptionResponse(0).getExpressionTree.getOperandsCount shouldBe validSearchSubscriptionResponse.head.getExpression.getOperands.asScala.length

    }


    "should retry in case of exception and throw an exception if all retries are exhausted" in {

      Given("Subscription Manager and invalid search subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      when(asyncHttpClient.executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))).thenReturn(
        failurePromise.future)
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)

      When("search subscription request is called")
      val future = subscriptionManager.searchSubscriptionRequest(getInvalidSearchSubscriptionRequest)

      Then("exception is thrown after exhausting all retries")
      val exception = intercept[Exception] {
        Await.result(future, (appConfiguration.subscriptionConfig.numOfRetries * appConfiguration.subscriptionConfig.retryInSeconds + 1) seconds)
      }
      exception should have message EXCEPTION
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries + 1)).executePost(any(classOf[String]), any(classOf[String]), any(classOf[Array[Byte]]))

    }

  }

  private def getValidSearchSubscriptionRequest: SearchSubscriptionRequest = {
    val labels = Map("servicename" -> "abc", "interval" -> "1m").asJava
    SearchSubscriptionRequest.newBuilder().putAllLabels(labels).build()
  }


  private def getInvalidSearchSubscriptionRequest: SearchSubscriptionRequest = {
    SearchSubscriptionRequest.newBuilder().build()
  }

  private def getValidSearchSubscriptionResponse: List[model.SubscriptionResponse] = {
    List(getValidGetSubscriptionResponse)
  }

}
