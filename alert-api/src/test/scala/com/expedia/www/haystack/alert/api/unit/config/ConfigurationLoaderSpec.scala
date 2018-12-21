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

package com.expedia.www.haystack.alert.api.unit.config

import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.unit.BasicUnitTestSpec

class ConfigurationLoaderSpec extends BasicUnitTestSpec {

  "Configuration Loader" should {

    "load service config from base.conf" in {
      val serviceConfiguration = new AppConfiguration().serviceConfig
      serviceConfiguration.port shouldBe 8088
      serviceConfiguration.ssl.enabled shouldBe false
      serviceConfiguration.ssl.certChainFilePath shouldBe ""
      serviceConfiguration.ssl.privateKeyPath shouldBe ""
    }

    "load client config from base.conf" in {
      val clientConfiguration = new AppConfiguration().clientConfig
      clientConfiguration.connectionTimeout shouldBe 30000
      clientConfiguration.socketTimeout shouldBe 30000
    }

    "load subscription config from base.conf" in {
      val subscriptionConfiguration = new AppConfiguration().subscriptionConfig
      subscriptionConfiguration.baseUrl shouldBe s"""http://wiremock:8500/subscriptions"""
      subscriptionConfiguration.numOfRetries shouldBe 2
      subscriptionConfiguration.retryInSeconds shouldBe 1
    }

  }


}
