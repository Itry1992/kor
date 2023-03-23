package kafka.server.netty

import com.tongtech.netty.buffer.ByteBuf
import com.tongtech.netty.channel.ChannelHandlerContext
import com.tongtech.netty.handler.codec.ByteToMessageDecoder
import com.tongtech.netty.util.ReferenceCountUtil

import java.util

class DecoderToRequest extends ByteToMessageDecoder {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    if (in.readableBytes() < 4)
      return
    val sizeBuf = ctx.alloc.buffer(4)
    in.readBytes(sizeBuf)
    val recevieSize = sizeBuf.readInt()
    ReferenceCountUtil.release(sizeBuf)
    if (recevieSize == 0) {
      out.add(None)
      return
    }
    if (in.readableBytes() < recevieSize) {
      return
    }
    //暂停处理数据，保证处理请求的顺序性
    ctx.channel().config().setAutoRead(false)
    val buf = ctx.alloc().buffer(recevieSize)
    in.readBytes(buf)
    out.add(new ReceiveRequest(recevieSize, buf))
  }
}
