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

package com.expedia.www.haystack.alert.api

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.ServiceLoader

import com.codahale.metrics.JmxReporter
import com.expedia.www.anomaly.store.backend.api.AnomalyStore
import com.expedia.www.haystack.alert.api.config.AppConfiguration
import com.expedia.www.haystack.alert.api.config.entities.PluginConfig
import com.expedia.www.haystack.alert.api.services.{AnomalyReaderService, GrpcHealthService, SubscriptionService}
import com.expedia.www.haystack.commons.logger.LoggerUtils
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

object App extends MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  implicit private val executor = scala.concurrent.ExecutionContext.global

  val config = new AppConfiguration
  var stores: List[AnomalyStore] = _

  def main(args: Array[String]): Unit = {
    //create an instance of the application
    val jmxReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
    startApp()
  }

  private def startApp() = {
    try {
      stores = loadAndInitializePlugins(config.pluginConfigs)
      startService()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        LOGGER.error("Fatal error observed while running the app", ex)
        LoggerUtils.shutdownLogger()
        System.exit(1)
    }
  }


  private def startService(): Unit = {
    val serviceConfig = config.serviceConfig

    val serverBuilder = NettyServerBuilder
      .forPort(serviceConfig.port)
      .directExecutor()
      .addService(new SubscriptionService(config)(executor))
      .addService(new AnomalyReaderService(stores)(executor))
      .addService(new GrpcHealthService())

    // enable ssl if enabled
    if (serviceConfig.ssl.enabled) {
      serverBuilder.useTransportSecurity(new File(serviceConfig.ssl.certChainFilePath), new File(serviceConfig.ssl.privateKeyPath))
    }

    val server = serverBuilder.build().start()

    LOGGER.info(s"server started, listening on ${serviceConfig.port}")

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        LOGGER.info("shutting down since JVM is shutting down")
        server.shutdown()
        stores.foreach(store => store.close())
        LOGGER.info("server has been shutdown now")
      }
    })

    server.awaitTermination()

  }

  @throws[Exception]
  private def loadAndInitializePlugins(configs: List[PluginConfig]): List[AnomalyStore] = {
    configs.map(loadAndInitializePlugin(_))
  }

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

}
