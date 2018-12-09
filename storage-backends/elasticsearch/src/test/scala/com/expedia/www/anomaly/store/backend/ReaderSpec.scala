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

import com.typesafe.config.ConfigFactory
import org.easymock.{Capture, EasyMock}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchResponseSections}
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}
import org.slf4j.LoggerFactory


class ReaderSpec extends FunSpec with Matchers with BeforeAndAfterEach {
  private val LOGGER = LoggerFactory.getLogger(classOf[Reader])

  private var mockClient: RestHighLevelClient = _
  private var capturedRequest: Capture[SearchRequest] = _
  private var capturedListener: Capture[ActionListener[SearchResponse]] = _
  private var reader: Reader = _
  private val timestamp = 1544353485L

  private val labels = Map("service" -> "svc1", "operation" -> "/foo")

  override def beforeEach(): Unit = {
    this.mockClient = EasyMock.mock(classOf[RestHighLevelClient])
    this.capturedRequest = EasyMock.newCapture()
    this.capturedListener = EasyMock.newCapture()

    mockClient.searchAsync(EasyMock.capture(capturedRequest), EasyMock.capture(capturedListener))
    reader = new Reader(mockClient, ConfigFactory.empty(), "haystack-anomalies", LOGGER)
  }

  override def afterEach(): Unit = {
    EasyMock.verify(this.mockClient)
  }

  describe("elastic search reader") {
    it("should handle the failure in elastic search response") {
      EasyMock.expectLastCall().andAnswer(() => {
        capturedListener.getValue.onFailure(new RuntimeException("fail to read from elastic!"))
        null
      })

      EasyMock.replay(mockClient)
      val expectWriteCallback =  new Array[Boolean](1)
      reader.read(labels, timestamp - 10000, timestamp, 100, (_, ex) => {
        if (ex == null) {
          fail("no exception is expected in searching alerts")
        }
        expectWriteCallback(0) = true
      })
      applyAsserts(expectWriteCallback(0))
    }

    it ("should handle the successful response") {
      EasyMock.expectLastCall().andAnswer(() => {
        capturedListener.getValue.onResponse(buildSearchResponse())
        null
      })

      EasyMock.replay(mockClient)
      val expectWriteCallback =  new Array[Boolean](1)
      reader.read(labels, timestamp - 10000, timestamp, 100, (anomalies, ex) => {
        if (ex != null) {
          fail("no exception is expected in searching alerts")
        }
        anomalies.size shouldBe 1
        expectWriteCallback(0) = true
      })
      applyAsserts(expectWriteCallback(0))
    }
  }

  private def applyAsserts(expectWriteCallback: Boolean): Unit = {
    expectWriteCallback shouldBe true
    val req = capturedRequest.getValue
    req.indices()(0) shouldEqual "haystack-anomalies*"
    req.source().toString shouldEqual
      """{
        |  "size" : 100,
        |  "timeout" : "15000ms",
        |  "query" : {
        |    "bool" : {
        |      "must" : [
        |        {
        |          "match" : {
        |            "labels.service" : {
        |              "query" : "svc1",
        |              "operator" : "OR",
        |              "prefix_length" : 0,
        |              "max_expansions" : 50,
        |              "fuzzy_transpositions" : true,
        |              "lenient" : false,
        |              "zero_terms_query" : "NONE",
        |              "boost" : 1.0
        |            }
        |          }
        |        },
        |        {
        |          "match" : {
        |            "labels.operation" : {
        |              "query" : "/foo",
        |              "operator" : "OR",
        |              "prefix_length" : 0,
        |              "max_expansions" : 50,
        |              "fuzzy_transpositions" : true,
        |              "lenient" : false,
        |              "zero_terms_query" : "NONE",
        |              "boost" : 1.0
        |            }
        |          }
        |        },
        |        {
        |          "range" : {
        |            "startTime" : {
        |              "from" : 1544343485,
        |              "to" : 1544353485,
        |              "include_lower" : false,
        |              "include_upper" : false,
        |              "boost" : 1.0
        |            }
        |          }
        |        }
        |      ],
        |      "adjust_pure_negative" : true,
        |      "boost" : 1.0
        |    }
        |  }
        |}""".stripMargin
  }


  private def buildSearchResponse(): SearchResponse = {
    val hits = new Array[SearchHit](1)
    hits(0) =  new SearchHit(1)
    new SearchResponse(
      new SearchResponseSections(
        new SearchHits(hits, 0L, 0),
      null, null, false, false, null, 0), "", 0, 0, 0, 0L, null)
  }
}