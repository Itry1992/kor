package kafka.server

import com.tong.kafka.common.message.ApiMessageType.ListenerType
import com.tong.kafka.common.protocol.ApiKeys
import com.tong.kafka.common.record.RecordVersion
import com.tong.kafka.common.requests.ApiVersionsResponse
import kafka.network
import kafka.network.RequestChannel

import java.util
import scala.jdk.CollectionConverters._

trait ApiVersionManager {
  def listenerType: ListenerType

  def enabledApis: collection.Set[ApiKeys]

  def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse

  def isApiEnabled(apiKey: ApiKeys): Boolean = enabledApis.contains(apiKey)

  def newRequestMetrics: RequestChannel.Metrics = new network.RequestChannel.Metrics(enabledApis)
}

object ApiVersionManager {

}

class BrokerApiVersionManager() extends ApiVersionManager {
  val listenerType: ListenerType = ListenerType.ZK_BROKER
  private val features = BrokerFeatures.createEmpty()


  override def apiVersionResponse(throttleTimeMs: Int): ApiVersionsResponse = {

    val supportedFeatures = features.supportedFeatures


    ApiVersionsResponse.createApiVersionsResponse(
      throttleTimeMs,
      RecordVersion.current(),
      supportedFeatures,
      new util.HashMap[String, java.lang.Short](0),
      -1,
      null,
      listenerType)
  }

  override def enabledApis: collection.Set[ApiKeys] = {
    ApiKeys.apisForListener(listenerType).asScala
  }

  override def isApiEnabled(apiKey: ApiKeys): Boolean = {
    apiKey.inScope(listenerType)
  }
}
