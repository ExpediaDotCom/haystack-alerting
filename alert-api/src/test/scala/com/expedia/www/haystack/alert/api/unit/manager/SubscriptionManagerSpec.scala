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
import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.serde.JacksonJsonSerde
import com.expedia.www.haystack.alert.api.unit.BasicUnitTestSpec
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}

import scala.collection.JavaConverters._

trait SubscriptionManagerSpec extends BasicUnitTestSpec {

  val OK_STATUS_CODE = 200
  val ERROR_STATUS_CODE = 500
  val ERROR_SUBSCRIPTION_RESULT = "Invalid create request"

  val appConfiguration = new AppConfiguration
  val serde = new JacksonJsonSerde

  def getHttpResponse(result: String, statusCode: Int): BasicHttpResponse = {
    val httpEntity = new ByteArrayEntity(result.getBytes)
    val basicHttpResponse = new BasicHttpResponse(new BasicStatusLine(PROTOCOL_VERSION, statusCode, null))
    basicHttpResponse.setEntity(httpEntity)
    basicHttpResponse
  }

  def getValidGetSubscriptionResponse : model.SubscriptionResponse = {
    val subscriptionResponse = new model.SubscriptionResponse()
    subscriptionResponse.setId("1")
    val user = new model.User()
    user.setId("haystack")
    subscriptionResponse.setUser(user)
    subscriptionResponse.setLastModifiedTime(2l)
    subscriptionResponse.setCreatedTime(1l)
    val dispatcher = new model.Dispatcher()
    dispatcher.setEndpoint("haystack")
    dispatcher.setType(model.Dispatcher.Type.SLACK)
    val dispatchers = List(dispatcher).asJava
    subscriptionResponse.setDispatchers(dispatchers)
    subscriptionResponse.setExpression(getSubscriptionResponseExpressionTree)
    subscriptionResponse
  }

  def getSubscriptionResponseExpressionTree: model.ExpressionTree = {
    val expressionTree = new model.ExpressionTree()
    expressionTree.setOperator(model.Operator.AND)
    val field1 = new model.Field()
    field1.setKey("product")
    field1.setValue("haystack")
    val operand = new model.Operand()
    operand.setField(field1)
    expressionTree.setOperands(List(operand).asJava)
    expressionTree
  }

}
