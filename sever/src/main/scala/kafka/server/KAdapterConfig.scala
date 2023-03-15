package kafka.server

import com.tong.kafka.common.config.ConfigDef
import com.tong.kafka.common.{Node, AdapterConfig => JAdapterConfig}
import kafka.server.KAdapterConfig._
import kafka.utils.Logging

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._


class KAdapterConfig private(doLog: Boolean, override val props: java.util.Map[_, _]) extends KafkaConfig(props = props, doLog = doLog, configDef = KAdapterConfig.configDef) with Logging with JAdapterConfig {

  this.logIdent = ""


  private def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, props)

  private def this(props: java.util.Map[_, _]) = this(true, props)

  private var nodes: Option[List[Node]] = None
  private var thisNode: Option[Node] = None

  /**
   * 配置文件中读取代理节点配置
   * 代理节点现在使用静态配置
   *
   * @return
   */
  def getAdapterBroker: List[Node] = {
    nodes.getOrElse(getNodesFormConfig)
  }

  /**
   * 获取当前节点的node信息
   *
   * @return
   */
  def getCurrentNode: Node = {
    if (thisNode.isEmpty) {
      getNodesFormConfig
    }
    thisNode.get
  }


  /**
   * 获取配置的Htp每次批量拉取消息数量
   *
   * @return
   */
  def getHtpPullBatchMums: Int = {
    this.getInt(KAdapterConfig.HtpPullBatchMums)
  }


  private def getNodesFormConfig: List[Node] = {
    val nodeId = Option(getInt(AdapterNodeId).toInt)
    val addr = Option(getString(AdapterListenAddress))
    val maybeStrings = addr.map(f => f.split(":").map(r => r.trim))
    var error = false
    if (nodeId.isEmpty || maybeStrings.map(r => r.length).getOrElse(0) != 2) {
      error = true
    }
    val host = maybeStrings.map(r => r(0))
    val port = maybeStrings.map(r => r(1).toInt)
    if (host.isEmpty || port.isEmpty) {
      error = true
    }
    if (error) {
      throw new RuntimeException(s"ERROR CONFIG: \n$AdapterNodeId:\t $AdapterNodeIdDoc \n${AdapterListenAddress}: \t ${AdapterListenAddressDoc}")
    }
    val listenNode = new Node(nodeId.get, host.get, port.get)
    thisNode = Option(listenNode)
    val value = mutable.ListBuffer(listenNode)
    val strings = getList(AdapterList)
    if (strings == null) {
      return value.toList
    }
    strings.asScala.map(getNodeFromString).foreach(n => {
      if (n.isDefined) {
        if (value.exists(node => node.id == n.get.id))
          throw new RuntimeException(s"ERROR CONFIG: 重复的代理服务器NodeId")
        value += n.get
      }
    })
    val node = value.sortBy(n => n.id()).toList
    nodes = Option(node)
    node
  }

  def getNodeFromString(addr: String): Option[Node] = {
    val list = addr.split(":").map(r => r.trim).toList
    if (list.length != 3)
      return None
    val nodeId = Option(list.head.toInt)
    val port = Option(list(3).toInt)
    if (nodeId.isEmpty || port.isEmpty)
      return None
    Option(new Node(nodeId.get, list(1), port.get))
  }

  override def getNameSrvProp: String = getString(NameSrvProp)

  override def getDomainName: String = getString(DomainName)

  override def getPoolIdleWaitingTimeMs: Integer = getInt(PoolIdleWaitingTime)

  override def getPollPeriodMs: Integer = getInt(PoolPeriodMs)

  override def getPoolMaxConsumerNums: Integer = getInt(PollMaxConsumerNums)

  override def getPoolMaxProduceNums: Integer = getInt(PollMaxProducerNums)

  override def getPoolMaxManagerNums: Integer = getInt(PoolMaxManagerNums)

  def getAdapterTopicMetadataCacheTimeMs: Integer = getInt(AdapterTopicMetadataCacheTimeMs)

  def getHtpMaxInflightAsyncRequestNums: Integer = getInt(HtpMaxInflightAsyncRequestNums)
}


object KAdapterConfig {

  //此处添加代理服务器需要配置的选项字段名
  //e.g. HtpProduceNums= htp.produce.nums;
  val HtpProduceNums = "htp.produce.nums"
  val HtpProduceNumsDoc = "Htp生产者数量"
  val AdapterList = "htp.adapter.servers"
  val AdapterListDoc = "其他代理服务器地址列表，地址格式这个列表的格式应该是nodeId1:host1:port1,nodeId2:host2:port2,...."
  val AdapterNodeId = "htp.adapter.nodeId"
  val AdapterNodeIdDoc = "代理服务器的节点Id"
  val AdapterListenAddress = "htp.adapter.listen"
  val AdapterListenAddressDoc = "代理服务器监听地址，格式HOST:PORT"
  val HtpPullBatchMums = "htp.pull.batch.nums"
  val HtpPullBatchMumsDoc = "代理服务器每次向htp拉取消息是,拉取消息的数量，建议根据消息大小和kafka 客户端最小拉取大小配置"
  val AdapterTopicMetadataCacheTimeMs = "adapter.topic.metadata.cache.time.ms";
  val AdapterTopicMetadataCacheTimeMsDoc = "代理服务器中，主题的元数据有效缓存时间，超过缓存时效之后，会重新向管理节点拉取";
  val HtpMaxInflightAsyncRequestNums = "htp.max.inflight.async.request"
  val HtpMaxInflightAsyncRequestDoc = "每个htp客户端最大异步请求的数量"
  val NameSrvProp = JAdapterConfig.NameSrvProp
  val DomainName = JAdapterConfig.DomainName
  val PoolPeriodMs = JAdapterConfig.PoolPeriodMs
  val PollMaxConsumerNums = JAdapterConfig.PollMaxConsumerNums
  val PoolMaxManagerNums = JAdapterConfig.PoolMaxManagerNums
  val PollMaxProducerNums = JAdapterConfig.PollMaxProducerNums
  val PoolIdleWaitingTime = JAdapterConfig.PoolIdleWaitingTime

  def fromProps(props: Properties): KAdapterConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): KAdapterConfig = {

    val config = new KAdapterConfig(props, doLog)
    val prop = config.getNameSrvProp
    if (prop == null) {
      throw new RuntimeException(s"must provide config of ${NameSrvProp} ")
    }
    config
  }

  var configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Type._
    KafkaConfig.configDef
      .define(HtpProduceNums, INT, 1, HIGH, HtpProduceNumsDoc)
      .define(AdapterList, LIST, null, HIGH, AdapterListDoc)
      .define(AdapterNodeId, INT, null, HIGH, AdapterNodeIdDoc)
      .define(AdapterListenAddress, STRING, "localhost:9999", HIGH, AdapterListenAddressDoc)
      .define(HtpPullBatchMums, INT, 20, MEDIUM, HtpPullBatchMumsDoc)
      .define(NameSrvProp, STRING, null, HIGH, "htp的管理节点通信地址 tcp://host:port")
      .define(DomainName, STRING, "domain1", HIGH, "代理节点向htp系统通信时，使用的域名")
      .define(PoolPeriodMs, INT, 1 * 60 * 1000, LOW, "htp客户端池清理空闲客户端或新增客户端的周期")
      .define(PollMaxConsumerNums, INT, 2, LOW, "htp客户端池中最大消费组的数量")
      .define(PoolMaxManagerNums, INT, 1, LOW, "htp客户端池中最大管理者的数量")
      .define(PollMaxProducerNums, INT, 2, LOW, "htp客户端池中最大生产者的数量")
      .define(PoolIdleWaitingTime, INT, 5 * 60 * 1000, LOW, "htp客户端每隔多久会被认为是空闲状态")
      .define(AdapterTopicMetadataCacheTimeMs, INT, 5 * 1000, LOW, AdapterTopicMetadataCacheTimeMsDoc)
      .define(HtpMaxInflightAsyncRequestNums, INT, 20, MEDIUM, HtpMaxInflightAsyncRequestDoc)
  }


}