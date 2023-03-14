package kafka.server

import com.tong.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import com.tong.kafka.common.utils.Utils
import com.tong.kafka.common.{Node, TopicPartition}

/**
 * 管理topic—partition 和 代理服务器的分配关系
 * kafka消息发送时，会向leader节点发送消息，同时拉取消息如果不指定优选节点，也是从leader节点拉取消息
 * 该类决定了消息的负载负载均衡
 * */
trait AdapterTopicManager {
  def getCoordinator(value: List[String], cType: CoordinatorType): List[Node]

  def getLeaderNode(topic: TopicPartition): Node

  def saveLeaderNode(topicPartition: TopicPartition, node: Node): Unit
}

class HashTopicManager(val nodes: List[Node]) extends AdapterTopicManager {
  private val sortNode = nodes.sortBy(n => n.id)
  private val total: Int = sortNode.length

  override def getLeaderNode(topic: TopicPartition): Node = {

    val mod = Utils.abs(topic.hashCode) % total
    sortNode(mod)
  }

  override def saveLeaderNode(topicPartition: TopicPartition, node: Node): Unit = {

  }

  override def getCoordinator(gruoupIds: List[String], cType: CoordinatorType): List[Node] = {
    gruoupIds.map(key => {
      val mod = Utils.abs(key.hashCode) % total
      sortNode(mod)
    })
  }


}
