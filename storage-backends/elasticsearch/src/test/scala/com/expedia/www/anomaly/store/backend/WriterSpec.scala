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

package com.expedia.www.anomaly.store.backend

import java.text.SimpleDateFormat
import java.util.Date

import com.expedia.www.anomaly.store.backend.api.{Anomaly, AnomalyWithId}
import com.typesafe.config.ConfigFactory
import org.easymock.{Capture, EasyMock}
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.{ActionListener, DocWriteRequest}
import org.elasticsearch.client.RestHighLevelClient
import org.scalatest.{BeforeAndAfterEach, FunSpec, GivenWhenThen, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class WriterSpec extends FunSpec with GivenWhenThen with Matchers with BeforeAndAfterEach {

    private val LOGGER = LoggerFactory.getLogger(classOf[Writer])
    private var mockClient: RestHighLevelClient = _
    private var capturedRequest: Capture[BulkRequest] = _
    private var capturedListener: Capture[ActionListener[BulkResponse]] = _
    private var writer: Writer = _

    override def beforeEach(): Unit = {
        this.mockClient = EasyMock.mock(classOf[RestHighLevelClient])
        this.capturedRequest = EasyMock.newCapture()
        this.capturedListener = EasyMock.newCapture()
        mockClient.bulkAsync(EasyMock.capture(capturedRequest), EasyMock.capture(capturedListener))
        val config = Map(Writer.MAX_RETRIES_CONFIG_KEY -> 1).asJava
        this.writer = new Writer(mockClient, ConfigFactory.parseMap(config), "haystack-anomalies", LOGGER)
    }

    override def afterEach(): Unit = {
        EasyMock.verify(this.mockClient)
    }

    describe("elastic search writer") {
        it("should retry if write failure happens") {
            EasyMock.expectLastCall().andAnswer(() => {
                val l = capturedListener.getValue.asInstanceOf[Writer#BulkActionListener]
                //throw exception for the first time and then send successful response
                if (l.getRetryCount == 0) {
                    l.onFailure(new RuntimeException("fail to index"))
                } else {
                    l.onResponse(buildBulkResponse())
                }
                null
            }).times(2)

            EasyMock.replay(mockClient)
            val expectWriteCallback = new Array[Boolean](1)
            writer.write(List(createAnomalyWithId), ex => {
                expectWriteCallback(0) = true
                if (ex != null) {
                    fail(ex.getMessage)
                }
            })
            applyAsserts(expectWriteCallback(0))
        }

        it ("should handle the failure") {
            EasyMock.expectLastCall().andAnswer(() => {
                capturedListener.getValue.onFailure(new RuntimeException("fail to index"))
                null
            }).times(2)

            EasyMock.replay(mockClient)
            val expectWriteCallback = new Array[Boolean](1)

            writer.write(List(createAnomalyWithId), ex => {
                expectWriteCallback(0) = true
                if (ex == null) {
                    fail("runtime indexing exception is expected")
                } else {
                    ex.getMessage shouldEqual "fail to index"
                }
            })
            applyAsserts(expectWriteCallback(0))
        }

        it("should handle the success response from writer") {
            EasyMock.expectLastCall().andAnswer(() => {
                capturedListener.getValue.onResponse(buildBulkResponse())
                null
            })
            EasyMock.replay(mockClient)
            val expectWriteCallback = new Array[Boolean](1)
            writer.write(List(createAnomalyWithId), ex => {
                expectWriteCallback(0) = true
                if (ex != null) {
                    fail(ex.getMessage)
                }
            })
            applyAsserts(expectWriteCallback(0))
        }
    }
    private def applyAsserts(expectWriteCallback: Boolean): Unit = {
        expectWriteCallback shouldBe true
        val requests = capturedRequest.getValue.requests()
        requests.size() shouldBe 1
        val request = requests.get(0).asInstanceOf[IndexRequest]
        request.id() shouldEqual "haystack-anomalies-0-1"
        request.index() shouldEqual expectedIndexName
        val indexBody: java.util.Map[String, Object] = request.sourceAsMap()

        indexBody.get(ElasticSearchStore.START_TIME).toString.toLong should be > ((System.currentTimeMillis() / 1000) - 30)
        indexBody.get(ElasticSearchStore.LABELS).asInstanceOf[java.util.Map[String, String]].get("service") shouldEqual "svc1"
    }

    private def expectedIndexName: String = {
        "haystack-anomalies-" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    }

    private def createAnomalyWithId: AnomalyWithId = {
        AnomalyWithId("haystack-anomalies-0-1", Anomaly(Map("service" -> "svc1").asJava, System.currentTimeMillis() / 1000))
    }

    private def buildBulkResponse(): BulkResponse = {
        val itemResponse = new BulkItemResponse(1, DocWriteRequest.OpType.INDEX, null.asInstanceOf[BulkItemResponse.Failure])
        new BulkResponse(Array[BulkItemResponse] {
            itemResponse
        }, 1000)
    }
}

