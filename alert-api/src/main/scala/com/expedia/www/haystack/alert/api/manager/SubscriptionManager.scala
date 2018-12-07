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

package com.expedia.www.haystack.alert.api.manager

import com.expedia.open.tracing.api.subscription._
import com.expedia.www.haystack.alert.api.client.AsyncHttpClient
import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.mapper.{RequestMapper, ResponseMapper}
import com.expedia.www.haystack.alert.api.serde.JacksonJsonSerde
import com.expedia.www.haystack.alert.api.utils.RetryOnError
import org.apache.http.util.EntityUtils

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContextExecutor, Future}

class SubscriptionManager(appConfiguration: AppConfiguration)(implicit val executor: ExecutionContextExecutor) extends RetryOnError {

  private val CONTENT_TYPE = "application/json"
  private val SEARCH_SUBSCRIPTION_SUFFIX = "/search"
  val httpClient = new AsyncHttpClient(appConfiguration.clientConfig)
  val jacksonJsonSerde = new JacksonJsonSerde
  val subscriptionBaseUrl = appConfiguration.subscriptionConfig.baseUrl
  val retryDuration: FiniteDuration = appConfiguration.subscriptionConfig.retryInSeconds.seconds
  val numberOfRetries = appConfiguration.subscriptionConfig.numOfRetries


  def createSubscriptionRequest(request: CreateSubscriptionRequest): Future[CreateSubscriptionResponse] = {

    val createSubscriptionRequest = RequestMapper.mapCreateSubscriptionRequest(request)

    retry(retryDuration, numberOfRetries) {
      httpClient.executePost(subscriptionBaseUrl, CONTENT_TYPE, jacksonJsonSerde.serialize(createSubscriptionRequest))
    }
      .map(response => {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode >= 200 && statusCode <= 299) {
          val subscriptionId = jacksonJsonSerde.deserialize[List[String]](EntityUtils.toByteArray(response.getEntity)).get.head
          CreateSubscriptionResponse.newBuilder().setSubscriptionId(subscriptionId).build()
        } else {
          throw new RuntimeException(s"Failed with status code $statusCode with error ${EntityUtils.toString(response.getEntity)}")
        }
      })

  }

  def updateSubscriptionRequest(request: UpdateSubscriptionRequest): Future[Empty] = {
    val updateSubscriptionRequest = RequestMapper.mapUpdateSubscriptionRequest(request)
    retry(retryDuration, numberOfRetries) {
      httpClient.executePut(subscriptionBaseUrl, CONTENT_TYPE, jacksonJsonSerde.serialize(updateSubscriptionRequest))
    }
      .map(response => {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode >= 200 && statusCode <= 299) {
          Empty.newBuilder().build()
        } else {
          //TODO add log statement & timer and count
          throw new RuntimeException(s"Failed with status code $statusCode with error ${EntityUtils.toString(response.getEntity)}")
        }
      })
  }

  def searchSubscriptionRequest(request: SearchSubscriptionRequest): Future[SearchSubscriptionResponse] = {
    val searchSubscriptionRequest = RequestMapper.mapSearchSubscriptionRequest(request)

    retry(retryDuration, numberOfRetries) {
      httpClient.executePost(s"$subscriptionBaseUrl${SEARCH_SUBSCRIPTION_SUFFIX}", CONTENT_TYPE, jacksonJsonSerde.serialize(searchSubscriptionRequest))
    }
      .map(response => {
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode >= 200 && statusCode <= 299) {
          //TODO fix option
          val searchSubscriptionResponse = jacksonJsonSerde.deserialize[List[com.expedia.alertmanager.model.SubscriptionResponse]](EntityUtils.toByteArray(response.getEntity))
          ResponseMapper.mapSearchSubscriptionResponse(searchSubscriptionResponse)
        } else {
          throw new RuntimeException(s"Failed with status code $statusCode with error ${EntityUtils.toString(response.getEntity)}")
        }
      })
  }

  def getSubscriptionRequest(request: GetSubscriptionRequest): Future[SubscriptionResponse] = {
    val getSubscriptionRequest = RequestMapper.mapGetSubscriptionRequest(request)
    val url = s"$subscriptionBaseUrl/$getSubscriptionRequest"
    retry(retryDuration, numberOfRetries) {
      httpClient.executeGet(url, CONTENT_TYPE)
    }.map(response => {
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode >= 200 && statusCode <= 299) {
        //TODO fix option
        val subscriptionResponse = jacksonJsonSerde.deserialize[com.expedia.alertmanager.model.SubscriptionResponse](EntityUtils.toByteArray(response.getEntity)).get
        ResponseMapper.mapSubscriptionResponse(subscriptionResponse)
      } else {
        throw new RuntimeException(s"Failed with status code $statusCode with error ${EntityUtils.toString(response.getEntity)}")
      }
    })
  }

  def deleteSubscriptionRequest(request: DeleteSubscriptionRequest): Future[Empty] = {
    val deleteSubscriptionRequest = RequestMapper.mapDeleteSubscriptionRequest(request)
    val url = s"$subscriptionBaseUrl/$deleteSubscriptionRequest"
    retry(retryDuration, numberOfRetries) {
      httpClient.executeDelete(url, CONTENT_TYPE)
    }.map(response => {
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode >= 200 && statusCode <= 299) {
        //TODO fix option
        Empty.newBuilder().build()
      } else {
        throw new RuntimeException(s"Failed with status code $statusCode with error ${EntityUtils.toString(response.getEntity)}")
      }
    })
  }


}