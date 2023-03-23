package kafka.server.netty

import com.tong.kafka.common.errors.InvalidRequestException
import com.tong.kafka.common.message.ApiVersionsRequestData
import com.tong.kafka.common.network.Send
import com.tong.kafka.common.protocol.ApiKeys
import com.tong.kafka.common.requests._
import com.tongtech.netty.buffer.ByteBuf
import com.tongtech.netty.channel.Channel
import com.tongtech.netty.util.ReferenceCountUtil
import kafka.server.ApiVersionManager
import kafka.utils.NotNothing

import java.nio.ByteBuffer
import scala.annotation.nowarn
import scala.reflect.ClassTag

class AdapterRequestContext private(val payload: ByteBuffer,
                                    val byteBuf: Option[ByteBuf],
                                    val apiVersionManager: ApiVersionManager,
                                    val channel: Channel,
                                    val fullId: String) {
  def buildResponseSend(response: AbstractResponse): Send = {
    response.toSend(header.toResponseHeader, apiVersion)
  }

  def buildResponseEnvelopePayload(response: AbstractResponse): ByteBuffer = {
    response.serializeWithHeader(header.toResponseHeader, apiVersion)
  }


  def release: Unit = {
    byteBuf match {
      case Some(byteBuf: ByteBuf) => ReferenceCountUtil.release(byteBuf)
      case None => _
    }
  }

  def apiVersion = {
    header.apiVersion()
  }

  val header = parseRequestHeader(payload)

  val requestAndSize = parseRequest(payload)

  def parseRequestHeader(payload: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(payload)
    if (apiVersionManager.isApiEnabled(header.apiKey)) {
      header
    } else {
      throw new InvalidRequestException(s"Received request api key ${header.apiKey} which is not enabled")
    }
  }

  def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
    requestAndSize.request match {
      case r: T => r
      case r =>
        throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
    }
  }


  private def isUnsupportedApiVersionsRequest = (header.apiKey eq ApiKeys.API_VERSIONS) && !ApiKeys.API_VERSIONS.isVersionSupported(header.apiVersion)

  def parseRequest(buffer: ByteBuffer): RequestAndSize = {
    if (isUnsupportedApiVersionsRequest) {
      // Unsupported ApiVersion requests are treated as v0 requests and are not parsed
      val apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData, 0.toShort, header.apiVersion)
      new RequestAndSize(apiVersionsRequest, 0)
    }
    else {
      val apiKey = header.apiKey
      try {
        val apiVersion = header.apiVersion
        AbstractRequest.parseRequest(apiKey, apiVersion, buffer)
      } catch {
        case ex: Throwable =>
          throw new InvalidRequestException("Error getting request for apiKey: " + apiKey + ", apiVersion: " + header.apiVersion + ", connectionId: " + fullId, ex)
      }
    }
  }


}

object AdapterRequestContext {
  def fromByteBuf(buffer: ByteBuf,
                  apiVersionManager: ApiVersionManager,
                  channel: Channel,
                  fullID: String): AdapterRequestContext = {
    if (buffer.isDirect) {
      new AdapterRequestContext(buffer.nioBuffer(), Some(buffer), apiVersionManager, channel, fullID)
    } else {
      val bytes = new Array[Byte](buffer.readableBytes)
      buffer.getBytes(buffer.readerIndex, bytes)
      val byteBuffer = ByteBuffer.wrap(bytes)
      ReferenceCountUtil.release(buffer)
      new AdapterRequestContext(byteBuffer, None, apiVersionManager, channel, fullID)
    }
  }
}
