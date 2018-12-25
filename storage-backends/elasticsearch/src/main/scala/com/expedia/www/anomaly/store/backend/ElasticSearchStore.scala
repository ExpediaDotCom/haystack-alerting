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

import java.io.{BufferedReader, InputStreamReader}
import java.util.Collections
import java.util.stream.Collectors

import com.expedia.www.anomaly.store.backend.api.{AnomalyStore, AnomalyWithId}
import com.typesafe.config.Config
import org.apache.http.HttpHost
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.{Response, RestClient, RestHighLevelClient}
import org.slf4j.LoggerFactory

object ElasticSearchStore {
  private[backend] val INDEX_PREFIX_CONFIG_KEY = "index.prefix"
  private[backend] val HOST_CONFIG_KEY = "host"
  private[backend] val TEMPLATE_CONFIG_KEY = "template"
  private[backend] val ES_INDEX_TYPE = "anomaly"
  private[backend] val START_TIME = "startTime"
  private[backend] val SERVICE_NAME = "serviceName"
  private[backend] val OPERATION_NAME = "operationName"
  private[backend] val TAGS = "tags"
  private[backend] val DEFAULT_INDEX_PREFIX = "haystack-anomalies"
}

class ElasticSearchStore extends AnomalyStore {
  import ElasticSearchStore._
  private val logger = LoggerFactory.getLogger(classOf[ElasticSearchStore])
  private var client: RestHighLevelClient = _
  private var reader: Reader = _
  private var writer: Writer = _

  override def read(labels: Map[String, String],
                    from: Long,
                    to: Long,
                    size: Int,
                    callback: AnomalyStore.ReadCallback): Unit = {
    reader.read(labels, from, to, size, callback)
  }

  override def write(anomalies: Seq[AnomalyWithId], callback: AnomalyStore.WriteCallback): Unit = {
    writer.write(anomalies, callback)
  }

  override def init(config: Config): Unit = {
    logger.info("Initializing elastic search store with config {}", config)

    val indexNamePrefix = if (config.hasPath(INDEX_PREFIX_CONFIG_KEY)) config.getString(INDEX_PREFIX_CONFIG_KEY) else ElasticSearchStore.DEFAULT_INDEX_PREFIX
    val esHost = if (config.hasPath(HOST_CONFIG_KEY)) config.getString(HOST_CONFIG_KEY) else "http://localhost:9200"

    this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create(esHost)))
    this.reader = new Reader(client, config, indexNamePrefix, logger)
    this.writer = new Writer(client, config, indexNamePrefix, logger)
    applyIndexTemplate(config)
  }

  private def applyIndexTemplate(config: Config) = {
    val template = if (config.hasPath(TEMPLATE_CONFIG_KEY)) config.getString(TEMPLATE_CONFIG_KEY) else resourceTemplate()

    if (!template.toString.isEmpty) {
      logger.info("Applying indexing template {}", template)
      val entity = new NStringEntity(template.toString, ContentType.APPLICATION_JSON)
      val resp: Response = client.getLowLevelClient.performRequest(
        "PUT",
        "/_template/alert-store-template",
        Collections.emptyMap[String, String](),
        entity)

      if (resp.getStatusLine == null ||
        (resp.getStatusLine.getStatusCode < 200 && resp.getStatusLine.getStatusCode >= 300)) {
        throw new RuntimeException(String.format("Fail to execute put template request '%s'", template.toString))
      }
      else {
        logger.info("indexing template has been successfully applied - '{}'", template)
      }
    }
  }

  override def close(): Unit = {
    /* close the client quietly */
    try {
      client.close()
    } catch {
      case ex: Exception =>
        logger.error("Fail to close elastic client with error", ex)
    }
  }

  private def resourceTemplate(): String = {
    var reader: BufferedReader = null
    try {
      reader = new BufferedReader(new InputStreamReader(this.getClass.getResourceAsStream("/index_template.json")))
      reader.lines.collect(Collectors.joining("\n"))
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }
}
