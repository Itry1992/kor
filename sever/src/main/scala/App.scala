import com.tong.kafka.common.utils.Utils
import kafka.server.{AdapterSever, KafkaConfig}

import java.util.Properties

object App {
  def getProps(): Properties = {

    val props = Utils.loadProps("server.properties")
    props
  }

  def main(args: Array[String]): Unit = {

    val config = KafkaConfig.fromProps(getProps())
    val sever = new AdapterSever(brokerId = 1, port = 9999, config = config)
    sever.startup();
  }
}
