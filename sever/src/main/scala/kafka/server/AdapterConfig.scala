package kafka.server

import com.tong.kafka.common.Node
import com.tong.kafka.common.config.ConfigDef
import kafka.server.AdapterConfig._
import kafka.utils.Logging

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._


class AdapterConfig(doLog: Boolean, override val props: java.util.Map[_, _]) extends KafkaConfig(props = props, doLog = doLog, configDef = AdapterConfig.configDef) with Logging {
  this.logIdent = ""


  def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, props)

  def this(props: java.util.Map[_, _]) = this(true, props)

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

  def getListenNode: Node = {
    if (thisNode.isEmpty) {
      getNodesFormConfig
    }
    thisNode.get
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
      throw new RuntimeException(s"ERROR CONFIG: \n${AdapterNodeId}:\t ${AdapterNodeIdDoc} \n${AdapterListenAddress}: \t ${AdapterListenAddressDoc}")
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
    val node = value.toList
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
}


object AdapterConfig {

  //此处添加代理服务器需要配置的选项字段名
  //e.g. TlqProduceNums= tlq.produce.nums;
  val TlqProduceNums = "tlq.produce.nums"
  val TlqProduceNumsDoc = "tlq生产者数量"
  val AdapterList = "tlq.adapter.servers"
  val AdapterListDoc = "其他代理服务器地址列表，地址格式这个列表的格式应该是nodeId1:host1:port1,nodeId2:host2:port2,...."
  val AdapterNodeId = "tlq.adapter.nodeId"
  val AdapterNodeIdDoc = "代理服务器的节点Id"
  val AdapterListenAddress = "tlq.adapter.listen"
  val AdapterListenAddressDoc = "代理服务器监听地址，格式HOST:PORT"


  def fromProps(props: Properties): AdapterConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): AdapterConfig =
    new AdapterConfig(props, doLog)

  var configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Type._
    KafkaConfig.configDef
      .define(TlqProduceNums, INT, 1, HIGH, TlqProduceNumsDoc)
      .define(AdapterList, LIST, null, HIGH, AdapterListDoc)
      .define(AdapterNodeId, INT, null, HIGH, AdapterNodeIdDoc)
      .define(AdapterListenAddress, STRING, "localhost:9999", HIGH, AdapterListenAddressDoc)
  }


}