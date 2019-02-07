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

package com.expedia.www.haystack.alert.api.manager

import com.expedia.open.tracing.api.anomaly.{SearchAnamoliesRequest, SearchAnomaliesResponse}
import com.expedia.www.anomaly.store.backend.api.{AnomalyStore, AnomalyWithId}
import com.expedia.www.haystack.alert.api.mapper.AnomalyMapper._
import com.expedia.www.haystack.alert.api.utils.FutureCompanion
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

class AnomalyReaderManager(stores: List[AnomalyStore])(implicit val executor: ExecutionContextExecutor) extends FutureCompanion {

  private val LOGGER = LoggerFactory.getLogger(classOf[AnomalyReaderManager])

  def getAnomalies(request: SearchAnamoliesRequest): Future[SearchAnomaliesResponse] = {
    val results: List[Future[Seq[AnomalyWithId]]] = stores.map(store => {
      val promise = Promise[Seq[AnomalyWithId]]()
      store.read(request.getLabelsMap.asScala.toMap, request.getStartTime, request.getEndTime, request.getSize,
        (anomalies: Seq[AnomalyWithId], ex: Exception) => {
          if (ex != null) {
            LOGGER.error(s"Error in fetching anomalies from store", ex)
            promise.failure(ex)
          } else {
            LOGGER.info(s"total anomalies are ${anomalies.toList.length}")
            promise.success(anomalies)
          }
        })
      promise.future
    })
    allSucceededAsTrys(results).map(getSearchAnomaliesResponse)
  }


}
