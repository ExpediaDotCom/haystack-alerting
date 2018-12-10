package com.expedia.www.haystack.subscription.manager

import java.io.File

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.logger.LoggerUtils
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.subscription.manager.config.AppConfiguration
import com.expedia.www.haystack.subscription.manager.services.{GrpcHealthService, SubscriptionService}
import io.grpc.netty.NettyServerBuilder
import org.slf4j.LoggerFactory

object App extends MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  implicit private val executor = scala.concurrent.ExecutionContext.global

  def main(args: Array[String]): Unit = {
    //create an instance of the application
    val jmxReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).build()

    startService()
  }


  private def startService(): Unit = {
    try {
      val config = new AppConfiguration

      val serviceConfig = config.serviceConfig

      val serverBuilder = NettyServerBuilder
        .forPort(serviceConfig.port)
        .directExecutor()
        .addService(new SubscriptionService(config)(executor))
        .addService(new GrpcHealthService())

      // enable ssl if enabled
      if (serviceConfig.ssl.enabled) {
        serverBuilder.useTransportSecurity(new File(serviceConfig.ssl.certChainFilePath), new File(serviceConfig.ssl.privateKeyPath))
      }

      val server = serverBuilder.build().start()

      LOGGER.info(s"server started, listening on ${serviceConfig.port}")

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          LOGGER.info("shutting down gRPC server since JVM is shutting down")
          server.shutdown()
          LOGGER.info("server has been shutdown now")
        }
      })

      server.awaitTermination()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        LOGGER.error("Fatal error observed while running the app", ex)
        LoggerUtils.shutdownLogger()
        System.exit(1)
    }
  }

}
