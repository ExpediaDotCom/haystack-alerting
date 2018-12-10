package com.expedia.www.anomaly.store.backend

import java.util.concurrent.TimeUnit

import com.expedia.www.anomaly.store.backend.ElasticSearchStore._
import com.expedia.www.anomaly.store.backend.api.{Anomaly, AnomalyStore, AnomalyWithId}
import com.typesafe.config.Config
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilders, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable

object Reader {
  private[backend] val DEFAULT_MAX_READ_SIZE = 10000
  private[backend] val DEFAULT_READ_TIMEOUT_MS = 15000
  private[backend] val MAX_READ_SIZE_CONFIG_KEY = "max.read.size"
  private[backend] val READ_TIMEOUT_MS_CONFIG_KEY = "read.timeout.ms"

  private def convertMapToAnomaly(sourceAsMap: java.util.Map[String, Object]): Anomaly = {
    val timestamp = sourceAsMap.get(START_TIME).toString.toLong
    val labels = sourceAsMap.get(TAGS).asInstanceOf[java.util.Map[String, String]].asScala.toMap
    Anomaly(labels.asJava, timestamp)
  }
}

class Reader private[backend](client: RestHighLevelClient,
                              config: Config,
                              indexNamePrefix: String,
                              logger: Logger) {
  import Reader._
  private val timeout = readTimeout(config)
  private val maxSize = maxReadSize(config)

  def read(labels: Map[String, Object], from: Long, to: Long, size: Int, callback: AnomalyStore.ReadCallback): Unit = {
    val sourceBuilder = new SearchSourceBuilder
    val boolQuery = QueryBuilders.boolQuery

    labels.foreach {
      case (key, value) => boolQuery.must(QueryBuilders.matchQuery(TAGS + '.' + key, value))
    }

    boolQuery.must(new RangeQueryBuilder(START_TIME).gt(from).lt(to))
    sourceBuilder.query(boolQuery).timeout(timeout).size(if (size == 0) maxSize else size)

    val searchRequest = new SearchRequest().source(sourceBuilder).indices(this.indexNamePrefix + "*")

    client.searchAsync(searchRequest, new ActionListener[SearchResponse] {
      override def onResponse(searchResponse: SearchResponse): Unit = {
        val anomalies = new mutable.ListBuffer[AnomalyWithId]
        for (hit <- searchResponse.getHits.getHits) {
          val aId =
            if (hit.getSourceAsMap != null) {
              AnomalyWithId(hit.getId, convertMapToAnomaly(hit.getSourceAsMap))
            } else {
              AnomalyWithId(hit.getId, Anomaly.empty)
            }
          anomalies += aId
        }
        callback.onComplete(anomalies.toList, null)
      }

      override def onFailure(ex: Exception): Unit = {
        logger.error("Fail to read the alert response from elastic search", ex)
        callback.onComplete(null, ex)
      }
    })
  }

  private def maxReadSize(config: Config): Int = {
    if (config.hasPath(MAX_READ_SIZE_CONFIG_KEY)) config.getInt(MAX_READ_SIZE_CONFIG_KEY) else DEFAULT_MAX_READ_SIZE
  }

  private def readTimeout(config: Config): TimeValue = {
    val timeout = if (config.hasPath(READ_TIMEOUT_MS_CONFIG_KEY)) config.getLong(READ_TIMEOUT_MS_CONFIG_KEY) else DEFAULT_READ_TIMEOUT_MS
    new TimeValue(timeout, TimeUnit.MILLISECONDS)
  }
}
