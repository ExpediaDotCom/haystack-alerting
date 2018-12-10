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

package com.expedia.www.haystack.subscription.manager.services

import com.expedia.open.tracing.api.subscription._
import com.expedia.www.haystack.subscription.manager.config.AppConfiguration
import com.expedia.www.haystack.subscription.manager.manager.SubscriptionManager
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContextExecutor

class SubscriptionService(appConfiguration: AppConfiguration)(implicit val executor: ExecutionContextExecutor) extends SubscriptionManagementGrpc.SubscriptionManagementImplBase {

  private val handleCreateSubscriptionResponse = new GrpcHandler(SubscriptionManagementGrpc.METHOD_CREATE_SUBSCRIPTION.getFullMethodName)
  private val handleUpdateSubscriptionResponse = new GrpcHandler(SubscriptionManagementGrpc.METHOD_UPDATE_SUBSCRIPTION.getFullMethodName)
  private val handleDeleteSubscriptionResponse = new GrpcHandler(SubscriptionManagementGrpc.METHOD_DELETE_SUBSCRIPTION.getFullMethodName)
  private val handleSearchSubscriptionResponse = new GrpcHandler(SubscriptionManagementGrpc.METHOD_SEARCH_SUBSCRIPTION.getFullMethodName)
  private val handleGetSubscriptionResponse = new GrpcHandler(SubscriptionManagementGrpc.METHOD_GET_SUBSCRIPTION.getFullMethodName)
  private val subscriptionManager = new SubscriptionManager(appConfiguration)

  override def createSubscription(request: CreateSubscriptionRequest, responseObserver: StreamObserver[CreateSubscriptionResponse]): Unit = {
    handleCreateSubscriptionResponse.handle(request, responseObserver) {
      subscriptionManager.createSubscriptionRequest(request)
    }
  }

  override def updateSubscription(request: UpdateSubscriptionRequest, responseObserver: StreamObserver[Empty]): Unit = {
    handleUpdateSubscriptionResponse.handle(request, responseObserver) {
      subscriptionManager.updateSubscriptionRequest(request)
    }
  }

  override def deleteSubscription(request: DeleteSubscriptionRequest, responseObserver: StreamObserver[Empty]): Unit = {
    handleDeleteSubscriptionResponse.handle(request, responseObserver) {
      subscriptionManager.deleteSubscriptionRequest(request)
    }
  }

  override def getSubscription(request: GetSubscriptionRequest, responseObserver: StreamObserver[SubscriptionResponse]): Unit = {
    handleGetSubscriptionResponse.handle(request, responseObserver) {
      subscriptionManager.getSubscriptionRequest(request)
    }
  }

  override def searchSubscription(request: SearchSubscriptionRequest, responseObserver: StreamObserver[SearchSubscriptionResponse]): Unit = {
    handleSearchSubscriptionResponse.handle(request, responseObserver) {
      subscriptionManager.searchSubscriptionRequest(request)
    }
  }
}
