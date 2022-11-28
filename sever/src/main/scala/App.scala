import com.tong.kafka.common.utils.Utils
import kafka.server.{AdapterSever, KafkaConfig}

import java.net.InetAddress
import java.util.Properties

object App {
  val port = 9999

  def getProps(): Properties = {

    val props = Utils.loadProps("server.properties")
    props
  }

  def main(args: Array[String]): Unit = {

    val config = KafkaConfig.fromProps(getProps())
    val sever = new AdapterSever(brokerId = 1, port = port, config = config)
    sever.startup();
  }
}
