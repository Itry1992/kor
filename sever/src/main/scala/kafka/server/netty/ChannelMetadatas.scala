package kafka.server.netty

import com.tong.kafka.common.network.ClientInformation

import java.util.concurrent.ConcurrentHashMap

class ChannelMetadatas {

  private val channelMetadatas = new ConcurrentHashMap[String, ChannelMetadata]()

  def getChannelMetadata(channelId: String): Option[ChannelMetadata] = {
    Option(channelMetadatas.get(channelId))
  }

  def remove(id: String): Unit = {
    channelMetadatas.remove(id)
  }

  def setMetaData(channlId: String, fullId: String, clientInfo: Option[ClientInformation]): Unit = {
    channelMetadatas.put(channlId, new ChannelMetadata(fullId, clientInfo))
  }

  class ChannelMetadata(
                         val fullId: String,
                         val clientInfo: Option[ClientInformation] = None
                       ) {

  }
}
