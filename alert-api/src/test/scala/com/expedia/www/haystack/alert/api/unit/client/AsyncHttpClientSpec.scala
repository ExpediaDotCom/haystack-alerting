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

package com.expedia.www.haystack.alert.api.unit.client

import java.util.concurrent.{CompletableFuture, Future}

import com.expedia.www.haystack.alert.api.client.AsyncHttpClient
import com.expedia.www.haystack.alert.api.unit.BasicUnitTestSpec
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}
import org.apache.http.util.EntityUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.concurrent.Await
import scala.concurrent.duration._

class AsyncHttpClientSpec extends BasicUnitTestSpec {

  private val URL = "some-url"
  private val CONTENT_TYPE = "application/json"
  private val RESULT = "answer"
  private val QUERY = "query"

  "Async Http Client" should {

    "execute post and return successful response" in {

      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(successfulAnswer)

      When("execute post is called")
      val future = asyncHttpClient.executePost(URL, CONTENT_TYPE, QUERY.getBytes)

      Then("request is completed successfully")
      val httpResponse = Await.result(future, 5 seconds)
      httpResponse.getStatusLine.getStatusCode shouldBe 200
      EntityUtils.toByteArray(httpResponse.getEntity) shouldBe RESULT.getBytes


    }

    "execute post and return an exception in case of failure" in {

      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(failedAnswer)

      When("execute post is called")
      val future = asyncHttpClient.executePost(URL, CONTENT_TYPE, QUERY.getBytes)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception.getMessage shouldBe EXCEPTION

    }

    "execute get and return successful response" in {
      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(successfulAnswer)

      When("execute get is called")
      val future = asyncHttpClient.executeGet(URL, CONTENT_TYPE)

      Then("request is completed successfully")
      val httpResponse = Await.result(future, 5 seconds)
      httpResponse.getStatusLine.getStatusCode shouldBe 200
      EntityUtils.toByteArray(httpResponse.getEntity) shouldBe RESULT.getBytes
    }

    "execute get and return an exception in case of failure" in {
      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(failedAnswer)

      When("execute get is called")
      val future = asyncHttpClient.executeGet(URL, CONTENT_TYPE)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception.getMessage shouldBe EXCEPTION
    }

    "execute delete and return a successful response" in {
      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(successfulAnswer)

      When("execute delete is called")
      val future = asyncHttpClient.executeDelete(URL, CONTENT_TYPE)

      Then("request is completed successfully")
      val httpResponse = Await.result(future, 5 seconds)
      httpResponse.getStatusLine.getStatusCode shouldBe 200
      EntityUtils.toByteArray(httpResponse.getEntity) shouldBe RESULT.getBytes

    }


    "execute delete and return an exception in case of failure" in {
      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(failedAnswer)

      When("execute delete is called")
      val future = asyncHttpClient.executeDelete(URL, CONTENT_TYPE)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception.getMessage shouldBe EXCEPTION
    }


    "execute put and return successful response" in {

      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(successfulAnswer)

      When("execute put is called")
      val future = asyncHttpClient.executePut(URL, CONTENT_TYPE, QUERY.getBytes)

      Then("request is completed successfully")
      val httpResponse = Await.result(future, 5 seconds)
      httpResponse.getStatusLine.getStatusCode shouldBe 200
      EntityUtils.toByteArray(httpResponse.getEntity) shouldBe RESULT.getBytes


    }

    "execute put and return an exception in case of failure" in {

      Given("Async Http Client")
      val asyncHttpClient = mockHttpClient(failedAnswer)

      When("execute put is called")
      val future = asyncHttpClient.executePut(URL, CONTENT_TYPE, QUERY.getBytes)

      Then("exception is thrown")
      val exception = intercept[Exception] {
        Await.result(future, 5 seconds)
      }
      exception.getMessage shouldBe EXCEPTION

    }


  }

  lazy val successfulAnswer = new Answer[Future[HttpResponse]] {
    override def answer(invocation: InvocationOnMock): Future[HttpResponse] = {
      val httpEntity = new ByteArrayEntity(RESULT.getBytes)
      val basicHttpResponse = new BasicHttpResponse(new BasicStatusLine(PROTOCOL_VERSION, 200, null))
      basicHttpResponse.setEntity(httpEntity)
      invocation.getArgument[FutureCallback[HttpResponse]](1).completed(basicHttpResponse)
      CompletableFuture.completedFuture(null)
    }
  }


  lazy val failedAnswer = new Answer[Future[HttpResponse]] {
    override def answer(invocation: InvocationOnMock): Future[HttpResponse] = {
      invocation.getArgument[FutureCallback[HttpResponse]](1).failed(new RuntimeException(EXCEPTION))
      CompletableFuture.completedFuture(null)
    }
  }

  private def mockHttpClient(callBackAnswer: Answer[Future[HttpResponse]]): AsyncHttpClient = {
    val httpClient = mock(classOf[CloseableHttpAsyncClient])
    val asyncHttpClient = new AsyncHttpClient(httpClient)
    when(httpClient.execute(any(classOf[HttpRequestBase]), any(classOf[FutureCallback[HttpResponse]]))).thenAnswer(
      callBackAnswer
    )
    asyncHttpClient
  }

}
