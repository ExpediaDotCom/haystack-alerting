/*
 * Copyright 2018 Expedia Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.expedia.www.anomaly.store

import com.expedia.www.anomaly.store.serde.AnomalyDeserializer
import org.scalatest.{FunSpec, Matchers}

class AnomalyDeserializerSpec extends FunSpec with Matchers {
    private val deser = new AnomalyDeserializer()
    describe("Anomaly deserializer") {
        it("should handle empty data deserialization") {
            deser.deserialize("", null) shouldBe null
            deser.deserialize("", Array.emptyByteArray) shouldBe null
        }

        it("should handle good anomaly data") {
            val anomalyJson = """{"metricData":{"metricDefinition":{"tags":{"kv":{"mtype":"gauge","stat":"count","unit":"metric","product":"haystack","interval":"FiveMinute","operationName":"op1","serviceName":"svc"},"v":[]},"meta":{"kv":{},"v":[]},"key":"failure-count"},"value":11472.0,"timestamp":1535094600},"detectorUuid":"0e1c3968-a9fe-33a7-9c89-37a2142b3051","detectorType":"ewma-detector","anomalyResult":{"detectorUUID":"0e1c3968-a9fe-33a7-9c89-37a2142b3051","metricData":{"metricDefinition":{"tags":{"kv":{"mtype":"gauge","stat":"count","unit":"metric","product":"haystack","interval":"FiveMinute","operationName":"op1","serviceName":"svc"},"v":[]},"meta":{"kv":{},"v":[]},"key":"failure-count"},"value":11472.0,"timestamp":1535094600},"anomalyLevel":"NORMAL","predicted":4769.614801738369,"thresholds":{"upperStrong":26046.309426122076,"upperWeak":20431.44230539696,"lowerStrong":-18872.627539678855,"lowerWeak":-13257.760418953738}}}"""
            val anomaly = deser.deserialize("", anomalyJson.getBytes("utf-8"))
            anomaly.timestamp shouldBe 1535094600l
            anomaly.tags.get("serviceName") shouldEqual "svc"
            anomaly.tags.get("operationName") shouldEqual "op1"
        }
    }
}
