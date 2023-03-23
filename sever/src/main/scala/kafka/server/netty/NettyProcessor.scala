package kafka.server.netty

import com.tong.kafka.common.network.ClientInformation
import com.tong.kafka.common.protocol.ApiKeys
import com.tong.kafka.common.requests._
import com.tong.kafka.common.utils.Time
import com.tongtech.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import kafka.server.ApiVersionManager
import kafka.utils.Logging

import java.net.InetSocketAddress

class NettyProcessor(apiVersionManager: ApiVersionManager,
                     time: Time,
                     channelMetadatas: ChannelMetadatas,
                     requestChannel: RequestChannel) extends ChannelDuplexHandler with Logging {

  def channelId(ctx: ChannelHandlerContext): String = {
    val maybeString = channelMetadatas.getChannelMetadata(ctx.channel().id().asShortText()).map(r => r.fullId)
    if (maybeString.isDefined) {
      maybeString.get
    } else {
      val localAddress = ctx.channel().localAddress().asInstanceOf[InetSocketAddress]
      val remoteAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
      s"${localAddress.getHostString}:${localAddress.getPort}-${remoteAddress.getHostString}:${remoteAddress.getPort}-${ctx.channel().id().asShortText()}"
    }
  }


  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    val request = msg.asInstanceOf[ReceiveRequest]
    val connectionId = channelId(ctx)
    val context = AdapterRequestContext.fromByteBuf(request.payload, apiVersionManager, ctx.channel(), connectionId)
    //构建请求，从receive 中获取buffer,然后根据APIKey进行反序列化
    val req = context
    val header = context.header
    // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
    // and version. It is done here to avoid wiring things up to the api layer.
    if (header.apiKey == ApiKeys.API_VERSIONS) {
      val apiVersionsRequest = req.body[ApiVersionsRequest]
      if (apiVersionsRequest.isValid) {
        channelMetadatas.setMetaData(ctx.channel().id().asShortText(), connectionId, Some(new ClientInformation(
          apiVersionsRequest.data.clientSoftwareName,
          apiVersionsRequest.data.clientSoftwareVersion)))
      }
    }
    //放入请求到requestChannel 请求队列中
    requestChannel.sendRequest(new RequestChannel.Request(context = context, startTimeNanos = time.nanoseconds(), envelope = None))

  }


  override def disconnect(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    channelMetadatas.remove(ctx.channel().id().asShortText())
  }

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
    val response: RequestChannel.Response = msg.asInstanceOf[RequestChannel.Response]
//    super.write(ctx,response,promise)
    response match {
      case e:RequestChannel.NoOpResponse=> e.request.updateRequestMetrics()
    }
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    if (logger.isErrorEnabled) {
      val maybeMetadata = channelMetadatas.getChannelMetadata(ctx.channel().id().asShortText())
      logger.error(s"some error happen on channel ${maybeMetadata.map(r => r.fullId)},connection will  close, error info $cause")
    }
    ctx.channel().close().addListener(_ => {
      channelMetadatas.remove(ctx.channel().id().asShortText())
    })

  }
}
