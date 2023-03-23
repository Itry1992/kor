package kafka.network

import com.tong.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import com.tong.kafka.common.metrics.Metrics
import com.tong.kafka.common.metrics.stats.Meter
import com.tong.kafka.common.utils.{LogContext, Time}
import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.network.SocketServer.MetricsGroup
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, KafkaConfig}
import kafka.utils.{CoreUtils, Logging}
import org.slf4j.event.Level

import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, TimeUnit}
import scala.jdk.CollectionConverters._

object SocketServer {
  val MetricsGroup = "socket-server-metrics"

  val ReconfigurableConfigs = Set(
    KafkaConfig.MaxConnectionsPerIpProp,
    KafkaConfig.MaxConnectionsPerIpOverridesProp,
    KafkaConfig.MaxConnectionsProp,
    KafkaConfig.MaxConnectionCreationRateProp)

  val ListenerReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp)

  def closeSocket(
                   channel: SocketChannel,
                   logging: Logging
                 ): Unit = {
    CoreUtils.swallow(channel.socket().close(), logging, Level.ERROR)
    CoreUtils.swallow(channel.close(), logging, Level.ERROR)
  }

  def chainFuture(sourceFuture: CompletableFuture[Void],
                  destinationFuture: CompletableFuture[Void]): Unit = {
    sourceFuture.whenComplete((_, t) => if (t != null) {
      destinationFuture.completeExceptionally(t)
    } else {
      destinationFuture.complete(null)
    })
  }
}

class SocketServer(val config: KafkaConfig,
                   val endpoints: List[EndPoint],
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider,
                   val apiVersionManager: ApiVersionManager) extends Logging with KafkaMetricsGroup {
  private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}")
  private val maxQueuedRequests = config.queuedMaxRequests
  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))

  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE
  private[this] val nextProcessorId: AtomicInteger = new AtomicInteger(0)
  val connectionQuotas = new ConnectionQuotas(config, time, metrics)
  val requestChannel = new RequestChannel(maxQueuedRequests, SampleAcceptor.MetricPrefix, time, apiVersionManager.newRequestMetrics)
  private[network] val acceptors = new ConcurrentHashMap[EndPoint, SampleAcceptor]()
  /**
   * True if the SocketServer is stopped. Must be accessed under the SocketServer lock.
   */
  private[network] var stopped = false

  newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
  newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)

  endpoints.foreach(createSampleAcceptorAndProcessors)

  def getNextProcessorId(): Int = {
    nextProcessorId.getAndIncrement()
  }


  def createSampleAcceptorAndProcessors(endpoint: EndPoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new payload plane acceptor and processors: SocketServer is stopped.")
    }
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(endpoint.listenerName.configPrefix)
    connectionQuotas.addListener(config, endpoint.listenerName)
    val isPrivilegedListener = true
    val dataPlaneAcceptor = createSampleAcceptor(endpoint, isPrivilegedListener, requestChannel)
    dataPlaneAcceptor.configure(parsedConfigs)
    acceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created payload-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
  }

  protected def createSampleAcceptor(endPoint: EndPoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel): SampleAcceptor = {
    new SampleAcceptor(this, endPoint, config, connectionQuotas, time, isPrivilegedListener, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
  }

  def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")
      acceptors.asScala.values.foreach(_.beginShutdown())
      acceptors.asScala.values.foreach(_.close())
      info("Stopped socket server request processors")
    }
  }

  def enableRequestProcessing(): Unit = this.synchronized {
    if (stopped) {
      throw new RuntimeException("Can't enable request processing: SocketServer is stopped.")
    }
    acceptors.asScala.values.foreach(_.startFuture.complete(null))
  }

}

