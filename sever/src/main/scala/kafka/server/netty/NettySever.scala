package kafka.server.netty

import com.tongtech.netty.bootstrap.ServerBootstrap
import com.tongtech.netty.channel.nio.NioEventLoopGroup
import com.tongtech.netty.channel.socket.SocketChannel
import com.tongtech.netty.channel.socket.nio.NioServerSocketChannel
import com.tongtech.netty.channel.{ChannelInitializer, ChannelOption}

class NettySever {
  private[netty] val serverBootstrap = new ServerBootstrap
  private val bossGroup = new NioEventLoopGroup // (1)

  private val workerGroup = new NioEventLoopGroup

  def startUp(): Unit = {
    serverBootstrap.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new DecoderToRequest)
        }
      })
      .childOption(ChannelOption.SO_KEEPALIVE, true)
  }

}
