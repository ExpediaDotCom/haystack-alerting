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

package com.expedia.www.haystack.alert.api.mapper

import com.expedia.alertmanager._
import com.expedia.alertmanager.model.User
import com.expedia.open.tracing.api.subscription._

import scala.collection.JavaConverters._

object RequestMapper extends ExpressionTreeMapper with AlertDispatcherMapper {

  def mapCreateSubscriptionRequest(request: CreateSubscriptionRequest): model.CreateSubscriptionRequest = {
    val createSubscriptionRequest = new model.CreateSubscriptionRequest()
    val user = new User
    user.setId(request.getUser.getUsername)
    createSubscriptionRequest.setUser(user)
    val alertDispatchers = mapAlertDispatcher(request.getSubscriptionRequest.getDispatchersList.asScala.toList)
    createSubscriptionRequest.setDispatchers(alertDispatchers.asJava)
    createSubscriptionRequest.setExpression(mapExpressionTree(request.getSubscriptionRequest.getExpressionTree))
    createSubscriptionRequest
  }

  def mapUpdateSubscriptionRequest(request : UpdateSubscriptionRequest) : model.UpdateSubscriptionRequest = {
    val updateSubscriptionRequest = new model.UpdateSubscriptionRequest()
    updateSubscriptionRequest.setId(request.getSubscriptionId)
    val alertDispatchers = mapAlertDispatcher(request.getSubscriptionRequest.getDispatchersList.asScala.toList)
    updateSubscriptionRequest.setDispatchers(alertDispatchers.asJava)
    updateSubscriptionRequest.setExpression(mapExpressionTree(request.getSubscriptionRequest.getExpressionTree))
    updateSubscriptionRequest
  }

  def mapDeleteSubscriptionRequest(request: DeleteSubscriptionRequest) : String = {
    request.getSubscriptionId
  }

  def mapGetSubscriptionRequest(request: GetSubscriptionRequest) : String = {
    request.getSubscriptionId
  }

  def mapSearchSubscriptionRequest(request: SearchSubscriptionRequest) : model.SearchSubscriptionRequest = {
    val searchSubscriptionRequest = new model.SearchSubscriptionRequest()
    //TODO fix this
    searchSubscriptionRequest.setUserId(request.getSubscriptionId)
    searchSubscriptionRequest.setLabels(request.getLabelsMap)
    searchSubscriptionRequest
  }


}
