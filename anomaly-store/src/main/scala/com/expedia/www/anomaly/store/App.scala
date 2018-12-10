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

import java.io.{Closeable, File}
import java.net.{URL, URLClassLoader}
import java.util.ServiceLoader

import com.expedia.www.anomaly.store.backend.api.AnomalyStore
import com.expedia.www.anomaly.store.config.{AppConfiguration, PluginConfig}
import org.slf4j.LoggerFactory

object App extends scala.App {
  private val LOGGER = LoggerFactory.getLogger(classOf[App])

  val appConfig = if (args.apply(0) != null && args.apply(0).nonEmpty) {
    new AppConfiguration(args.apply(0))
  } else {
    new AppConfiguration()
  }

  val store = loadAndInitializePlugin(appConfig.pluginConfig)
  val controller = new AnomalyStoreController(appConfig.kafkaConfig, store, new HealthController(appConfig.healthStatusFilePath))

  controller.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    LOGGER.info("Shutdown hook is invoked, tearing down the application.")
    closeQuietly(controller)
    store.close()
  }))

  @throws[Exception]
  private def loadAndInitializePlugin(cfg: PluginConfig): AnomalyStore = {
    val pluginName = cfg.name
    val pluginJarFileName = cfg.jarName.toLowerCase
    LOGGER.info("Loading the store plugin with name={}", pluginName)

    val plugins = new File(cfg.directory).listFiles(_.getName.toLowerCase == pluginJarFileName)

    if (plugins == null || plugins.length != 1) {
      throw new RuntimeException(String.format("Fail to find the plugin with jarName=%s in the directory=%s", pluginJarFileName, cfg.directory))
    }

    val urls = Array[URL](plugins(0).toURI.toURL)
    val ucl = new URLClassLoader(urls, AnomalyStore.getClass.getClassLoader)

    val loader = ServiceLoader.load(classOf[AnomalyStore], ucl)
    // load and initialize the plugin
    val store = loader.iterator.next

    store.init(cfg.conf)
    LOGGER.info("Store plugin with name={} has been successfully loaded", pluginName)
    store
  }

  private def closeQuietly(closeable: Closeable) = {
    try
      closeable.close()
    catch {
      case _: Exception => /* do nothing */
    }
  }
}