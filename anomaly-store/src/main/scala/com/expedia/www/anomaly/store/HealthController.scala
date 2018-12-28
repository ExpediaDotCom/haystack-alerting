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

import java.io.IOException
import java.nio.file.{Files, Paths}

import org.slf4j.LoggerFactory

class HealthController(statusFile: String) {

  private val LOGGER = LoggerFactory.getLogger(classOf[HealthController])

  private[store] def setUnhealthy() = {
    write("false")
  }

  private[store] def setHealthy() = {
    write("true")
  }

  private def write(status: String) = {
    try
      Files.write(Paths.get(statusFile), status.getBytes)
    catch {
      case ex: IOException =>
        LOGGER.error("Exception while writing health status file", ex)
    }
  }
}
