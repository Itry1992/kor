package adapter.test

import adapter.test.mock.MockData
import com.tong.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import com.tong.kafka.common.record.CompressionType
import com.tong.kafka.common.serialization.StringSerializer
import com.tong.kafka.common.utils.Time
import com.tongtech.client.remoting.common.NettySystemConfig
import kafka.server.KAdapterConfig
import org.junit.{After, Before, Test}

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutionException, Future}

class AdapterMockTest {
  private[test] var mockSever: Option[MockSever] = None
  private[test] val severHost: String = "localhost:9999"
  val NameSrvPropAddr = "tcp://172.27.168.140:9888"

  private[test] val mockData: MockData = new MockData

  private[test] val topic: String = mockData.topics.get(0)

  @Before def startApp(): Unit = {
    val properties: Properties = new Properties
    properties.setProperty(KAdapterConfig.AdapterNodeId, "0")
    properties.setProperty(KAdapterConfig.AdapterListenAddress, severHost)
    properties.setProperty(KAdapterConfig.NameSrvProp, NameSrvPropAddr)
    val config: KAdapterConfig = KAdapterConfig.fromProps(properties)
    val htpMaxInflightAsyncRequestNums: Integer = config.getHtpMaxInflightAsyncRequestNums
    System.setProperty(NettySystemConfig.COM_TLQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, htpMaxInflightAsyncRequestNums.toString)
    mockSever = Some(new MockSever(Time.SYSTEM, 1, config.getCurrentNode, config))
    mockSever.get.startup()
  }

  @Test def SendTest(): Unit = {
    val props: Properties = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, util.Arrays.asList("localhost:9999"))
    //acks=0：如果设置为0，生产者不会等待kafka的响应。
    //acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
    //acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这		是最强的可用性保证。
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    //配置为大于0的值的话，客户端会在消息发送失败时重新发送。
    //        props.put("retries", 0);
    //当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。
    //        props.put("batch.size", 16384);
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name)
    try {
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      try for (i <- 0 until 10000) {
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, "999", "abc")
        val send: Future[RecordMetadata] = producer.send(record)
        if (i % 5 == 0) {
          val recordMetadata: RecordMetadata = send.get
          System.out.println(recordMetadata.toString)
        }
      }
      catch {
        case e: ExecutionException =>
          throw new RuntimeException(e)
        case e: InterruptedException =>
          throw new RuntimeException(e)
      } finally {
        if (producer != null) producer.close()
      }
    }
  }

  @After def stop(): Unit = {
    mockSever match {
      case Some(e) => e.shutdown()
      case _ =>
    }
  }
}
