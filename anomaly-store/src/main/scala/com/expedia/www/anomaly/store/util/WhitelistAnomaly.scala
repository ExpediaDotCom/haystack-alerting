/*
 *               Copyright 2019 Expedia, Inc.
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

package com.expedia.www.anomaly.store.util

import com.expedia.www.anomaly.store.backend.api.AnomalyWithId
import com.expedia.www.haystack.alerting.commons.AnomalyTagKeys
import com.expedia.www.haystack.commons.entities.TagKeys

object WhitelistAnomaly {

  private val PRODUCT_VALUE = "haystack"
  private val STRONG_ANOMALY_LEVEL = "strong"
  private val WEAK_ANOMALY_LEVEL = "weak"

  val necessaryTags = Map(TagKeys.PRODUCT_KEY -> List(PRODUCT_VALUE),
    AnomalyTagKeys.ANOMALY_LEVEL -> List(STRONG_ANOMALY_LEVEL, WEAK_ANOMALY_LEVEL))

  def shouldWhitelistAnomaly(anomalyWithId: AnomalyWithId): Boolean = {
    necessaryTags.keys.forall(tagKey =>
      anomalyWithId.anomaly.tags.containsKey(tagKey) &&
        necessaryTags(tagKey).exists(value => value.equalsIgnoreCase(anomalyWithId.anomaly.tags.get(tagKey))))
  }
}
