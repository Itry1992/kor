package kafka.server

import com.tong.kafka.common.{Node, TopicPartition}

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
  override def getLeaderNode(topic: TopicPartition): Node = {

    val mod = topic.hashCode % total
    sortNode(mod)
  }

  override def saveLeaderNode(topicPartition: TopicPartition, node: Node): Unit = {

  }
}
