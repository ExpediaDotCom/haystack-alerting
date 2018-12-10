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

package com.expedia.www.haystack.alert.api.client

import com.expedia.www.haystack.alert.api.config.entities.ClientConfiguration
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.nio.client.HttpAsyncClients

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

class AsyncHttpClient(clientConfiguration: ClientConfiguration)(implicit val executor: ExecutionContextExecutor) {

  val requestConfig = RequestConfig.custom()
    .setSocketTimeout(clientConfiguration.socketTimeout)
    .setConnectTimeout(clientConfiguration.connectionTimeout)
    .build()

  private val httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build()
  httpClient.start()


  def executePost(url: String, contentType: String, content: Array[Byte]): Future[HttpResponse] = {
    val httpPost = new HttpPost(url)
    httpPost.addHeader("Content-type", contentType)
    val entity = new ByteArrayEntity(content)
    httpPost.setEntity(entity)
    val promise = Promise[HttpResponse]()
    executeRequest(httpPost, promise)
    promise.future
  }


  def executePut(url: String, contentType: String, content: Array[Byte]): Future[HttpResponse] = {
    val httpPut = new HttpPut(url)
    httpPut.addHeader("Content-type", contentType)
    val entity = new ByteArrayEntity(content)
    httpPut.setEntity(entity)
    val promise = Promise[HttpResponse]()
    executeRequest(httpPut, promise)
    promise.future
  }


  def executeGet(url: String, contentType: String): Future[HttpResponse] = {
    val httpGet = new HttpGet(url)
    httpGet.addHeader("Content-type", contentType)
    val promise = Promise[HttpResponse]()
    executeRequest(httpGet, promise)
    promise.future
  }


  def executeDelete(url: String, contentType: String): Future[HttpResponse] = {
    val httpDelete = new HttpDelete(url)
    httpDelete.addHeader("Content-type", contentType)
    val promise = Promise[HttpResponse]()
    executeRequest(httpDelete, promise)
    promise.future
  }


  private def executeRequest(httpRequestBase: HttpRequestBase, promise: Promise[HttpResponse]) = {
    httpClient.execute(httpRequestBase, new FutureCallback[HttpResponse] {
      override def completed(t: HttpResponse): Unit = {
        promise.success(t)
      }

      override def failed(e: Exception): Unit = {
        promise.failure(e)
      }

      override def cancelled(): Unit = {
        promise.failure(new RuntimeException("Request is cancelled by the user"))
      }
    })
  }


}
