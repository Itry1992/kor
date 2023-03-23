package kafka.server.netty

import com.tongtech.netty.buffer.ByteBuf

class ReceiveRequest private[server](val size: Int, val payload: ByteBuf) {

}
