import com.tong.kafka.common.utils.Utils
import com.tongtech.client.remoting.common.NettySystemConfig
import kafka.server.{AdapterSever, KAdapterConfig}

import java.util.Properties

object App {

  def getProps(): Properties = {

    val props = Utils.loadProps("server.properties")
    props.setProperty(KAdapterConfig.AdapterNodeId, "0")
    props.setProperty(KAdapterConfig.AdapterListenAddress, "localhost:9999")
    props
  }

  def main(args: Array[String]): Unit = {
    val config = KAdapterConfig.fromProps(getProps())
    val inflightAsyncRequest = config.getHtpMaxInflightAsyncRequestNums
    System.setProperty(NettySystemConfig.COM_TLQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, inflightAsyncRequest.toString)
    val sever = new AdapterSever(brokerId = config.brokerId, host = config.getCurrentNode, config = config)
    sever.startup();
  }
}
