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

package com.expedia.www.haystack.alert.api.unit.manager.subscription

import com.expedia.open.tracing.api.subscription._
import com.expedia.www.haystack.alert.api.client.AsyncHttpClient
import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.manager.SubscriptionManager
import org.apache.http.HttpResponse
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

class DeleteSubscriptionSpec extends SubscriptionManagerSpec {

  private val VALID_SUBSCRIPTION_RESULT = ""


  "Subscription Manager " should {

    "should delete subscription in case of valid delete subscription request" in {

      Given("Subscription Manager and valid update subscription request")
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executeDelete(any(classOf[String]), any(classOf[String]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("delete subscription request is called")
      val future = subscriptionManager.deleteSubscriptionRequest(getValidDeleteSubscriptionRequest)

      Then("subscription is deleted")
      val updateSubscriptionResponse = Await.result(future, 5 seconds)
      verify(asyncHttpClient, times(1)).executeDelete(any(classOf[String]), any(classOf[String]))
      updateSubscriptionResponse should not be (null)


    }


    "should throw an exception in case of invalid delete subscription request" in {

      Given("Subscription Manager and invalid delete subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val promise = Promise[HttpResponse]()
      when(asyncHttpClient.executeDelete(any(classOf[String]), any(classOf[String]))).thenReturn(
        promise.future
      )
      promise.success(getHttpResponse(ERROR_SUBSCRIPTION_RESULT, ERROR_STATUS_CODE))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)

      When("delete subscription request is called")
      val future = subscriptionManager.deleteSubscriptionRequest(getInvalidDeleteSubscriptionRequest)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception should have message s"Failed with status code ${ERROR_STATUS_CODE} with error ${ERROR_SUBSCRIPTION_RESULT}"
      verify(asyncHttpClient, times(1)).executeDelete(any(classOf[String]), any(classOf[String]))


    }


    "should retry in case of exception and delete subscription in case of valid delete subscription request" in {

      Given("Subscription Manager and valid delete subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      val successPromise = Promise[HttpResponse]()
      when(asyncHttpClient.executeDelete(any(classOf[String]), any(classOf[String]))).thenReturn(
        failurePromise.future).thenReturn(successPromise.future)
      successPromise.success(getHttpResponse(VALID_SUBSCRIPTION_RESULT, OK_STATUS_CODE))
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)


      When("delete subscription request is called")
      val future = subscriptionManager.deleteSubscriptionRequest(getValidDeleteSubscriptionRequest)

      Then("subscription is updated after retrying")
      val exception = intercept[Exception] {
        Await.result(future, appConfiguration.subscriptionConfig.retryInSeconds - 1 seconds)
      }
      val deleteSubscriptionResponse = Await.result(future, 5 seconds)
      exception should have message s"Futures timed out after [${appConfiguration.subscriptionConfig.retryInSeconds - 1} seconds]"
      deleteSubscriptionResponse should not be (null)
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries)).executeDelete(any(classOf[String]), any(classOf[String]))

    }


    "should retry in case of exception and throw an exception if all retries are exhausted" in {

      Given("Subscription Manager and invalid delete subscription request")
      val appConfiguration = new AppConfiguration()
      val asyncHttpClient = mock(classOf[AsyncHttpClient])
      val failurePromise = Promise[HttpResponse]()
      when(asyncHttpClient.executeDelete(any(classOf[String]), any(classOf[String]))).thenReturn(
        failurePromise.future)
      failurePromise.failure(new RuntimeException(EXCEPTION))
      val subscriptionManager = new SubscriptionManager(appConfiguration, asyncHttpClient)

      When("delete subscription request is called")
      val future = subscriptionManager.deleteSubscriptionRequest(getInvalidDeleteSubscriptionRequest)

      Then("exception is thrown after exhausting all retries")
      val exception = intercept[Exception] {
        Await.result(future, (appConfiguration.subscriptionConfig.numOfRetries * appConfiguration.subscriptionConfig.retryInSeconds + 1) seconds)
      }
      exception should have message EXCEPTION
      verify(asyncHttpClient, times(appConfiguration.subscriptionConfig.numOfRetries + 1)).executeDelete(any(classOf[String]), any(classOf[String]))

    }

  }

  private def getValidDeleteSubscriptionRequest: DeleteSubscriptionRequest = {
    DeleteSubscriptionRequest.newBuilder().setSubscriptionId("1").build()
  }


  private def getInvalidDeleteSubscriptionRequest: DeleteSubscriptionRequest = {
    DeleteSubscriptionRequest.newBuilder().build()
  }

}
