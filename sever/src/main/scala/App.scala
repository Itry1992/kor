import com.tong.kafka.common.utils.Utils
import kafka.server.{AdapterConfig, AdapterSever}

import java.util.Properties

object App {

  def getProps(): Properties = {

    val props = Utils.loadProps("server.properties")
    props.setProperty(AdapterConfig.AdapterNodeId, "0")
    props.setProperty(AdapterConfig.AdapterListenAddress, "localhost:9999")
    props
  }

  def main(args: Array[String]): Unit = {

    val config = AdapterConfig.fromProps(getProps())
    val sever = new AdapterSever(brokerId = config.brokerId, host = config.getCurrentNode, config = config)
    sever.startup();
  }
}
