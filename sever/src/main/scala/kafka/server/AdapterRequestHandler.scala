package kafka.server

import com.tong.kafka.common.protocol.{ApiKeys, Errors}
import com.tong.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse}
import kafka.network.RequestChannel
import kafka.utils.Logging

class AdapterRequestHandler(val requestChannel: RequestChannel, apiVersionManager: ApiVersionManager) extends ApiRequestHandler with Logging {
  this.logIdent = "AdapterRequestHandler"

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
      s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

    request.header.apiKey match {
      case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
      case _ => None
    }
  }


  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs)
      }
    }

    requestChannel.sendResponse(request, createResponseCallback(0), None)

  }
}
