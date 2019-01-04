package com.expedia.www.anomaly.store.backend

import java.text.SimpleDateFormat
import java.util.Date

import com.expedia.www.anomaly.store.backend.ElasticSearchStore._
import com.expedia.www.anomaly.store.backend.api.AnomalyStore.WriteCallback
import com.expedia.www.anomaly.store.backend.api.{Anomaly, AnomalyWithId}
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.typesafe.config.Config
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentFactory
import org.slf4j.Logger
import com.codahale.metrics.Timer

object Writer {
  private[backend] val MAX_RETRIES_CONFIG_KEY = "max.retries"
  private[backend] val MAX_RETRY_BACKOFF_CONFIG_KEY = "retry.backoff.ms"
  private[backend] val DEFAULT_RETRY_BACKOFF_MS = 200l
  private[backend] val DEFAULT_MAX_RETRIES = 50
  private[backend] val INDEX_NAME_DATE_PATTERN = "yyyy-MM-dd"
}

class Writer private[backend](client: RestHighLevelClient,
                              config: Config,
                              indexNamePrefix: String,
                              logger: Logger) extends MetricsSupport {
  import Writer._

  // meter that measures the write failures
  private val esWriteFailureMeter = metricRegistry.meter("es.write.failure")

  // a timer that measures the amount of time it takes to complete one bulk write
  private val esWriteTime = metricRegistry.timer("es.writer.time")

  private val maxRetries = if (config.hasPath(MAX_RETRIES_CONFIG_KEY)) config.getInt(MAX_RETRIES_CONFIG_KEY) else  DEFAULT_MAX_RETRIES
  private val retryBackOffMillis = if (config.hasPath(MAX_RETRY_BACKOFF_CONFIG_KEY)) config.getLong(MAX_RETRY_BACKOFF_CONFIG_KEY) else DEFAULT_RETRY_BACKOFF_MS

  def write(anomalies: Seq[AnomalyWithId], callback: WriteCallback): Unit = {
    val bulkRequest = new BulkRequest
    val formatter = new SimpleDateFormat(INDEX_NAME_DATE_PATTERN)
    try {
      for (al <- anomalies) {
        val idxName = indexName(formatter, al.anomaly.timestamp)
        val indexRequest = new IndexRequest(idxName, ES_INDEX_TYPE, al.id)
        indexRequest.source(convertAnomalyToJsonMap(al.anomaly))
        bulkRequest.add(indexRequest)
      }
      this.client.bulkAsync(bulkRequest, new BulkActionListener(esWriteTime.time(), bulkRequest, callback, 0))
    } catch {
      case ex: Exception => callback.onComplete(ex)
    }
  }

  private[backend] class BulkActionListener(timer : Timer.Context ,
                                            bulkRequest: BulkRequest,
                                            callback: WriteCallback,
                                            retryCount: Int) extends ActionListener[BulkResponse] {
    def onResponse(bulkItemResponses: BulkResponse): Unit = {
      if (bulkItemResponses.hasFailures) {
        timer.close()
        retry(new RuntimeException("Fail to execute the elastic search write with partial failures:" + bulkItemResponses.buildFailureMessage))
      }
      else {
        timer.close()
        callback.onComplete(null)
      }
    }

    def onFailure(e: Exception): Unit = retry(e)

    private def retry(e: Exception) = {
      if (retryCount < maxRetries) try {
        Thread.sleep(retryBackOffMillis)
        client.bulkAsync(bulkRequest, new BulkActionListener(esWriteTime.time(), bulkRequest, callback, retryCount + 1))
      } catch {
        case e: Exception =>
          callback.onComplete(e)
      }
      else {
        esWriteFailureMeter.mark()
        logger.error("All retries while writing to elastic search have been exhausted")
        callback.onComplete(e)
      }
    }

    // visible for testing
    def getRetryCount: Int = retryCount
  }

  private def convertAnomalyToJsonMap(anomaly: Anomaly) = {
    XContentFactory
      .jsonBuilder
      .startObject
      .field(START_TIME, anomaly.timestamp)
      .field(TAGS, anomaly.tags)
      .endObject
  }

  private def indexName(formatter: SimpleDateFormat, startTime: Long) = {
    String.format("%s-%s", indexNamePrefix, formatter.format(new Date(startTime * 1000)))
  }
}
