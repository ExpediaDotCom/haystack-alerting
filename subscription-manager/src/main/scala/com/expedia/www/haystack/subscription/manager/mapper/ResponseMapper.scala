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

package com.expedia.www.haystack.subscription.manager.mapper

import com.expedia.open.tracing.api.subscription._

import scala.collection.JavaConverters._

object ResponseMapper extends AlertDispatcherMapper with ExpressionTreeMapper {

  def mapSearchSubscriptionResponse(response: Option[List[com.expedia.alertmanager.model.SubscriptionResponse]]): SearchSubscriptionResponse = {
    if (response.isDefined) {
      val searchSubscriptionResponses: List[SubscriptionResponse] = response.get.map(mapSubscriptionResponse(_))
      SearchSubscriptionResponse.newBuilder().addAllSubscriptionResponse(searchSubscriptionResponses.asJava).build()
    } else {
      SearchSubscriptionResponse.newBuilder().build()
    }
  }

  def mapSubscriptionResponse(response: com.expedia.alertmanager.model.SubscriptionResponse): SubscriptionResponse = {
    val subscriptionResponseBuilder = SubscriptionResponse.newBuilder()
    subscriptionResponseBuilder.setSubscriptionId(response.getId)
    subscriptionResponseBuilder.setUser(User.newBuilder().setUsername(response.getUser.getId))
    subscriptionResponseBuilder.addAllDispatchers(getDispatchers(response.getDispatchers.asScala.toList).asJava)
    subscriptionResponseBuilder.setExpressionTree(getExpressionTree(response.getExpression))
    subscriptionResponseBuilder.setLastModifiedTime(response.getLastModifiedTime)
    subscriptionResponseBuilder.setCreatedTime(response.getCreatedTime)
    subscriptionResponseBuilder.build()
  }

  def getFieldOperand(field: com.expedia.alertmanager.model.Field): Operand = {
    val operandField = Field.newBuilder().setName(field.getKey).setValue(field.getValue).build()
    Operand.newBuilder().setField(operandField).build()
  }

  def getExpressionTree(expressionTree: com.expedia.alertmanager.model.ExpressionTree): ExpressionTree = {
    val expressionBuilder = ExpressionTree.newBuilder()

    val operands = expressionTree.getOperands.asScala.map(operand => {
      operand.getExpression match {
        case null => getFieldOperand(operand.getField)
        case expression: com.expedia.alertmanager.model.ExpressionTree =>
          Operand.newBuilder().setExpression(getExpressionTree(expression)).build()
      }
    }).toList


    expressionBuilder.setOperator(getOperator(expressionTree.getOperator))
    expressionBuilder.addAllOperands(operands.asJava)
    expressionBuilder.build()

  }


}
