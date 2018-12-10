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
            val anomalyJson = "{\"metricData\":{\"metricDefinition\":{\"key\":null,\"tags\":{\"kv\":{\"mtype\":\"rate\",\"operationName\":\"/foo\",\"unit\":\"someunit\",\"service\":\"testapp\"},\"v\":[],\"empty\":false},\"meta\":{\"kv\":{},\"v\":[],\"empty\":true}},\"value\":10.0,\"timestamp\":1544354976}}"
            val anomaly = deser.deserialize("", anomalyJson.getBytes("utf-8"))
            anomaly.timestamp shouldBe 1544354976l
            anomaly.tags.get("service") shouldEqual "testapp"
            anomaly.tags.get("operationName") shouldEqual "/foo"
        }
    }
}