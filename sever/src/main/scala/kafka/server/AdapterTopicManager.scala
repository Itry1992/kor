package kafka.server

import com.tong.kafka.common.{Node, TopicPartition}

import java.util.concurrent.ConcurrentHashMap

/**
 * 管理topic—partition 和 代理服务器的分配关系
 * kafka消息发送时，会向leader节点发送消息，同时拉取消息如果不指定优选节点，也是从leader节点拉取消息
 * 该类决定了负载均衡
 */
trait AdapterTopicManager {
  def getLeaderNode(topic: TopicPartition): Node

  def saveLeaderNode(topicPartition: TopicPartition, node: Node): Unit
}

class HashTopicManager(val nodes: List[Node]) extends AdapterTopicManager {
  private val sortNode = nodes.sortBy(n => n.id)
  private val total: Int = sortNode.length
  private val map = new ConcurrentHashMap[TopicPartition, Node]()

  override def getLeaderNode(topic: TopicPartition): Node = {
    def getNext = {
      val mod = topic.hashCode % total
      sortNode(mod)
    }

    Option(map.get(topic)).getOrElse(getNext)
  }

  override def saveLeaderNode(topicPartition: TopicPartition, node: Node): Unit = {
    map.put(topicPartition, node)
  }
}
