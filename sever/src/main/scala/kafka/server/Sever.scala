package kafka.server

import com.tong.kafka.clients.CommonClientConfigs
import com.tong.kafka.common.metrics._
import com.tong.kafka.common.network.ListenerName
import com.tong.kafka.common.security.auth.SecurityProtocol
import com.tong.kafka.common.security.scram.internals.ScramMechanism
import com.tong.kafka.common.security.token.delegation.internals.DelegationTokenCache
import com.tong.kafka.common.utils.{LogContext, Time}
import com.tong.kafka.common.{AdapterScheduler, AdapterSchedulerImpl, Node}
import com.tong.kafka.consumer.AdapterConsumer
import com.tong.kafka.manager.AdapterManager
import com.tong.kafka.produce.AdapterProduce
import com.tong.kafka.server.common.BrokerState
import com.tong.kafka.tlq.TlqPool
import kafka.cluster.EndPoint
import kafka.group.GroupCoordinator
import kafka.network.{SampleAcceptor, SocketServer}
import kafka.quota.QuotaFactory
import kafka.security.CredentialProvider
import kafka.server.Sever.{BrokerIdLabel, ClusterIdLabel, MetricsPrefix}
import kafka.utils.{CoreUtils, Logging}

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

trait Server {
  def startup(): Unit

  def shutdown(): Unit

  def awaitShutdown(): Unit
}

class AdapterSever(time: Time = Time.SYSTEM, brokerId: Int, host: Node, val config: KAdapterConfig) extends Server with Logging {
  var quotaManagers: QuotaFactory.QuotaManagers = null
  protected val startupComplete = new AtomicBoolean(false)
  protected val isShuttingDown = new AtomicBoolean(false)
  protected val isStartingUp = new AtomicBoolean(false)
  @volatile protected var _brokerState: BrokerState = BrokerState.NOT_RUNNING
  protected var shutdownLatch = new CountDownLatch(1)
  protected var logContext: LogContext = null
  var socketServer: SocketServer = null
  var metrics: Metrics = null
  var requestHandlerPool: KafkaRequestHandlerPool = null
  var tlqPool: TlqPool = null
  var adapterScheduler: AdapterScheduler = null;

  override def startup(): Unit = {
    info("starting")

    if (isShuttingDown.get)
      throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

    if (startupComplete.get)
      return


    val canStartup = isStartingUp.compareAndSet(false, true)
    if (canStartup) {
      _brokerState = BrokerState.STARTING
      metrics = initializeMetrics(config, time, brokerId.toString)
      val tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      val credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)
      val apiVersionManager = new AdapterBrokerApiVersionManager()
      val point = new EndPoint(host.host(), host.port(), new ListenerName("kafka_adapter_listener"), SecurityProtocol.PLAINTEXT)
      socketServer = new SocketServer(config = config, endpoints = List(point), metrics, time = time, credentialProvider, apiVersionManager)
      val topicManager = new HashTopicManager(config.getAdapterBroker)
      //      val tlqManager = new MockManager
      //      val tlqProduce = new MockProduce()
      //      val tlqConsumer = new MockConsumer(tlqProduce, tlqManager)
      adapterScheduler = new AdapterSchedulerImpl(1)
      tlqPool = new TlqPool(adapterScheduler, config)
      val tlqManager = new AdapterManager(tlqPool, config.getAdapterTopicMetadataCacheTimeMs)
      try {
        tlqManager.isDomainExist(config.getDomainName)
      } catch {
        case e: Exception => {
          error(s"查询通信域失败，请检查配置项 ${KAdapterConfig.DomainName}", e)
          isStartingUp.set(false);
          shutdown()
        }
      }
      val tlqProduce = new AdapterProduce(tlqPool, tlqManager)
      val tlqConsumer = new AdapterConsumer(tlqManager, tlqPool, adapterScheduler)
      val coordinator = GroupCoordinator(config, Time.SYSTEM, topicManager)
      coordinator.startup()
      quotaManagers = QuotaFactory.instantiate(config, metrics, time, "quota_managers")

      val apiHandler = new AdapterRequestHandler(
        socketServer.requestChannel,
        apiVersionManager,
        time,
        config,
        tlqManager,
        tlqProduce = tlqProduce,
        topicManager = topicManager,
        tlqConsumer = tlqConsumer,
        groupCoordinator = coordinator,
        quotas = quotaManagers
      )
      requestHandlerPool = new KafkaRequestHandlerPool(brokerId = brokerId, requestChannel = socketServer.requestChannel, apis = apiHandler, time, numThreads = config.numIoThreads, requestHandlerAvgIdleMetricName = s"requestHandlerAvgIdleMetric", logAndThreadNamePrefix = s"${SampleAcceptor.ThreadPrefix}")
      socketServer.enableRequestProcessing()
      _brokerState = BrokerState.RUNNING
      startupComplete.set(true)
      isStartingUp.set(false)
    }
  }

  override def shutdown(): Unit = {
    if (isStartingUp.get)
      throw new IllegalStateException("server is still starting up, cannot shut down!")
    if (isShuttingDown.compareAndSet(false, true) && startupComplete.get()) {
      if (socketServer != null) {
        socketServer.stopProcessingRequests()
      }
      if (requestHandlerPool != null)
        CoreUtils.swallow(requestHandlerPool.shutdown(), this)
      if (metrics != null)
        CoreUtils.swallow(metrics.close(), this)
      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)
      if (tlqPool != null) {
        CoreUtils.swallow(tlqPool.close(), this)
      }
      if (adapterScheduler != null) {
        CoreUtils.swallow(adapterScheduler.shutdown(), this)
      }
      startupComplete.set(false)
      isShuttingDown.set(false)
      shutdownLatch.countDown()
      _brokerState = BrokerState.SHUTTING_DOWN
      info("shut down completed")
    }
  }

  override def awaitShutdown(): Unit = {

    shutdownLatch.await()
  }

  def initializeMetrics(
                         config: KafkaConfig,
                         time: Time,
                         clusterId: String
                       ): Metrics = {
    val metricsContext = createKafkaMetricsContext(config, clusterId)
    buildMetrics(config, time, metricsContext)
  }

  private def buildMetrics(
                            config: KafkaConfig,
                            time: Time,
                            metricsContext: KafkaMetricsContext
                          ): Metrics = {
    val defaultReporters = initializeDefaultReporters(config)
    val metricConfig = buildMetricsConfig(config)
    new Metrics(metricConfig, defaultReporters, time, true, metricsContext)
  }

  private def initializeDefaultReporters(
                                          config: KafkaConfig
                                        ): java.util.List[MetricsReporter] = {
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new java.util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)
    reporters
  }

  def buildMetricsConfig(
                          kafkaConfig: KafkaConfig
                        ): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  private[server] def createKafkaMetricsContext(
                                                 config: KafkaConfig,
                                                 clusterId: String
                                               ): KafkaMetricsContext = {
    val contextLabels = new java.util.HashMap[String, Object]
    contextLabels.put(ClusterIdLabel, clusterId)


    contextLabels.put(BrokerIdLabel, config.brokerId.toString)


    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    new KafkaMetricsContext(MetricsPrefix, contextLabels)
  }

}

object Sever {
  val MetricsPrefix: String = "kafka.server"
  val ClusterIdLabel: String = "kafka.cluster.id"
  val BrokerIdLabel: String = "kafka.broker.id"
  val NodeIdLabel: String = "kafka.node.id"
}

