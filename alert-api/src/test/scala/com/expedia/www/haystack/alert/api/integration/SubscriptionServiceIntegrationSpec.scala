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

package com.expedia.www.haystack.alert.api.integration

import com.expedia.www.haystack.alert.api.IntegrationSuite
import io.grpc.health.v1.{HealthCheckRequest, HealthCheckResponse}

import scala.collection.JavaConverters._

@IntegrationSuite
class SubscriptionServiceIntegrationSpec extends BasicIntegrationTestSpec {

  "Subscription Service Spec" should {

    "should return SERVING as health check response" in {
      val request = HealthCheckRequest.newBuilder().build()
      val response = healthCheckClient.check(request)
      response.getStatus shouldEqual HealthCheckResponse.ServingStatus.SERVING
    }

    "should create subscription" in {
      val createSubscriptionResponse = client.createSubscription(getCreateSubscriptionRequest())
      createSubscriptionResponse.getSubscriptionId shouldBe "1"
    }

    "should update subscription" in {
      val updateSubscriptionResponse = client.updateSubscription(getUpdateSubscriptionRequest)
      updateSubscriptionResponse should not be (null)
    }

    "should delete subscription" in {
      val deleteSubscriptionResponse = client.deleteSubscription(getDeleteSubscriptionRequest)
      deleteSubscriptionResponse should not be (null)
    }

    "should get subscription given subscription id" in {
      val subscriptionResponse = client.getSubscription(getGetSubscriptionRequest)
      subscriptionResponse should not be (null)
      subscriptionResponse.getSubscriptionId shouldBe "1"
      subscriptionResponse.getCreatedTime shouldBe 1
      subscriptionResponse.getLastModifiedTime shouldBe 2
      subscriptionResponse.getDispatchersList.asScala.length shouldBe 1
      subscriptionResponse.getDispatchersList.asScala.head.getEndpoint.toLowerCase shouldBe "haystack"
      subscriptionResponse.getExpressionTree.getOperandsCount shouldBe 1
    }

    "should search for subscription given some labels" in {
      val validSubscriptionResponses = client.searchSubscription(getSearchSubscriptionRequest)
      validSubscriptionResponses should not be (null)
      val validSubscriptionResponse = validSubscriptionResponses.getSubscriptionResponse(0)
      validSubscriptionResponses.getSubscriptionResponseCount shouldBe 1
      validSubscriptionResponse.getSubscriptionId shouldBe "1"
      validSubscriptionResponse.getCreatedTime shouldBe 1
      validSubscriptionResponse.getLastModifiedTime shouldBe 2
      validSubscriptionResponse.getDispatchersList.asScala.length shouldBe 1
      validSubscriptionResponse.getDispatchersList.asScala.head.getEndpoint.toLowerCase shouldBe "haystack"
      validSubscriptionResponse.getExpressionTree.getOperandsCount shouldBe 1
    }


  }

}
