package adapter.test

import adapter.test.mock.MockManager
import com.tong.kafka.common.network.ListenerName
import com.tong.kafka.common.security.auth.SecurityProtocol
import com.tong.kafka.common.security.scram.internals.ScramMechanism
import com.tong.kafka.common.security.token.delegation.internals.DelegationTokenCache
import com.tong.kafka.common.utils.Time
import com.tong.kafka.common.{AdapterSchedulerImpl, Node}
import com.tong.kafka.produce.AdapterProduce
import com.tong.kafka.server.common.BrokerState
import com.tong.kafka.tlq.TlqPool
import kafka.cluster.EndPoint
import kafka.group.GroupCoordinator
import kafka.network.{SampleAcceptor, SocketServer}
import kafka.quota.QuotaFactory
import kafka.security.CredentialProvider
import kafka.server._

class MockSever(time: Time = Time.SYSTEM, brokerId: Int, host: Node, override val config: KAdapterConfig) extends AdapterSever(time = time, brokerId = brokerId, host = host, config = config) {
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
      val tlqManager = new MockManager
      //      val tlqProduce = new MockProduce()
      //      val tlqConsumer = new MockConsumer(tlqProduce, tlqManager)
      adapterScheduler = new AdapterSchedulerImpl(1)
      tlqPool = new TlqPool(adapterScheduler, config)
      //      val tlqManager = new AdapterManager(tlqPool, config.getAdapterTopicMetadataCacheTimeMs)
      val tlqProduce = new AdapterProduce(tlqPool, tlqManager)
      val tlqConsumer = new MockConsumer(tlqManager, tlqPool, adapterScheduler)
      //      val tlqConsumer = new AdapterConsumer(tlqManager, tlqPool, adapterScheduler)
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
      isStartingUp.set(false)
      startupComplete.set(true)
    }
  }
}
