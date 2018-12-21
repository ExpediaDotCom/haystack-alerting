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
import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.manager.SubscriptionManager
import org.apache.http.HttpResponse
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}


class GetSubscriptionSpec extends SubscriptionManagerSpec {


  private val VALID_SUBSCRIPTION_RESULT = new String(serde.serialize(getValidGetSubscriptionResponse))


  "Subscription Manager " should {

    "should get subscription in case of valid get subscription request" in {

      Given("Subscription Manager and valid get subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executeGet(any(classOf[String]), any(classOf[String]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("get subscription request is called")
      val future = subscriptionManager.getSubscriptionRequest(getValidGetSubscriptionRequest)

      Then("subscription response is returned")
      val subscriptionResponse = Await.result(future, 5 seconds)
      verify(asyncHttpClient, times(1)).executeGet(any(classOf[String]), any(classOf[String]))
      subscriptionResponse should not be (null)
      val validSubscriptionResponse = getValidGetSubscriptionResponse
      subscriptionResponse.getSubscriptionId shouldBe validSubscriptionResponse.getId
      subscriptionResponse.getCreatedTime shouldBe validSubscriptionResponse.getCreatedTime
      subscriptionResponse.getLastModifiedTime shouldBe validSubscriptionResponse.getLastModifiedTime
      subscriptionResponse.getDispatchersList.asScala.length shouldBe validSubscriptionResponse.getDispatchers.asScala.length
      subscriptionResponse.getDispatchersList.asScala.head.getEndpoint shouldBe validSubscriptionResponse.getDispatchers.asScala.head.getEndpoint
      subscriptionResponse.getExpressionTree.getOperandsCount shouldBe validSubscriptionResponse.getExpression.getOperands.asScala.length


    }


    "should throw an exception in case of invalid get subscription request" in {

      Given("Subscription Manager and invalid get subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executeGet(any(classOf[String]), any(classOf[String]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(ERROR_SUBSCRIPTION_RESULT, ERROR_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)

      When("get subscription request is called")
      val future = subscriptionManager.getSubscriptionRequest(getInvalidGetSubscriptionRequest)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception should have message s"Failed with status code ${ERROR_STATUS_CODE} with error ${ERROR_SUBSCRIPTION_RESULT}"
      verify(asyncHttpClient, times(1)).executeGet(any(classOf[String]), any(classOf[String]))


    }


    "should retry in case of exception and get subscription in case of valid delete subscription request" in {

      Given("Subscription Manager and valid get subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      val successPromise = Promise[HttpResponse]()
      when(asyncHttpClient.executeGet(any(classOf[String]), any(classOf[String]))).thenReturn(
        failurePromise.future).thenReturn(successPromise.future)
      successPromise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("get subscription request is called")
      val future = subscriptionManager.getSubscriptionRequest(getValidGetSubscriptionRequest)

      Then("subscription is fetched after retrying")
      val exception = intercept[Exception] {
        Await.result(future, appConfiguration.subscriptionConfig.retryInSeconds - 1 seconds)
      }
      val subscriptionResponse = Await.result(future, 5 seconds)
      exception should have message s"Futures timed out after [${appConfiguration.subscriptionConfig.retryInSeconds - 1} seconds]"
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries)).executeGet(any(classOf[String]), any(classOf[String]))
      subscriptionResponse should not be (null)
      val validSubscriptionResponse = getValidGetSubscriptionResponse
      subscriptionResponse.getSubscriptionId shouldBe validSubscriptionResponse.getId
      subscriptionResponse.getCreatedTime shouldBe validSubscriptionResponse.getCreatedTime
      subscriptionResponse.getLastModifiedTime shouldBe validSubscriptionResponse.getLastModifiedTime
      subscriptionResponse.getDispatchersList.asScala.length shouldBe validSubscriptionResponse.getDispatchers.asScala.length
      subscriptionResponse.getDispatchersList.asScala.head.getEndpoint shouldBe validSubscriptionResponse.getDispatchers.asScala.head.getEndpoint
      subscriptionResponse.getExpressionTree.getOperandsCount shouldBe validSubscriptionResponse.getExpression.getOperands.asScala.length

    }


    "should retry in case of exception and throw an exception if all retries are exhausted" in {

      Given("Subscription Manager and invalid get subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      when(asyncHttpClient.executeGet(any(classOf[String]), any(classOf[String]))).thenReturn(
        failurePromise.future)
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)

      When("get subscription request is called")
      val future = subscriptionManager.getSubscriptionRequest(getInvalidGetSubscriptionRequest)

      Then("exception is thrown after exhausting all retries")
      val exception = intercept[Exception] {
        Await.result(future, (appConfiguration.subscriptionConfig.numOfRetries * appConfiguration.subscriptionConfig.retryInSeconds + 1) seconds)
      }
      exception should have message EXCEPTION
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries + 1)).executeGet(any(classOf[String]), any(classOf[String]))

    }

  }

  private def getValidGetSubscriptionRequest: GetSubscriptionRequest = {
    GetSubscriptionRequest.newBuilder().setSubscriptionId("1").build()
  }


  private def getInvalidGetSubscriptionRequest: GetSubscriptionRequest = {
    GetSubscriptionRequest.newBuilder().build()
  }

}
