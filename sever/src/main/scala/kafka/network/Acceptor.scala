package kafka.network

import com.tong.kafka.common.KafkaException
import com.tong.kafka.common.memory.MemoryPool
import com.tong.kafka.common.metrics.Metrics
import com.tong.kafka.common.network._
import com.tong.kafka.common.security.auth.SecurityProtocol
import com.tong.kafka.common.utils.{KafkaThread, LogContext, Time, Utils}
import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.network.Processor.ListenerMetricTag
import kafka.network.SocketServer.closeSocket
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, KafkaConfig}
import kafka.utils.{CoreUtils, Logging}
import org.slf4j.event.Level

import java.io.IOException
import java.net.{InetSocketAddress, SocketException}
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] abstract class Acceptor(val socketServer: SocketServer,
                                       val endPoint: EndPoint,
                                       var config: KafkaConfig,
                                       val connectionQuotas: ConnectionQuotas,
                                       time: Time,
                                       isPrivilegedListener: Boolean,
                                       requestChannel: RequestChannel,
                                       metrics: Metrics,
                                       credentialProvider: CredentialProvider,
                                       logContext: LogContext,
                                       memoryPool: MemoryPool,
                                       apiVersionManager: ApiVersionManager)
  extends Runnable with Logging with KafkaMetricsGroup {

  val shouldRun = new AtomicBoolean(true)

  def metricPrefix(): String

  def threadPrefix(): String

  private val sendBufferSize = config.socketSendBufferBytes
  private val recvBufferSize = config.socketReceiveBufferBytes
  private val listenBacklogSize = config.socketListenBacklogSize

  private val nioSelector = NSelector.open()
  private[network] val serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)
  private[network] val processors = new ArrayBuffer[Processor]()
  // Build the metric name explicitly in order to keep the existing name for compatibility
  private val blockedPercentMeterMetricName = explicitMetricName(
    "kafka.network",
    "Acceptor",
    s"${metricPrefix()}AcceptorBlockedPercent",
    Map(ListenerMetricTag -> endPoint.listenerName.value))
  private val blockedPercentMeter = newMeter(blockedPercentMeterMetricName, "blocked time", TimeUnit.NANOSECONDS)
  private var currentProcessorIndex = 0
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()
  private var started = false
  private[network] val startFuture = new CompletableFuture[Void]()

  val thread = KafkaThread.nonDaemon(
    s"${threadPrefix()}-kafka-socket-acceptor-${endPoint.listenerName}-${endPoint.securityProtocol}-${endPoint.port}",
    this)

  startFuture.thenRun(() => synchronized {
    if (!shouldRun.get()) {
      debug(s"Ignoring start future for ${endPoint.listenerName} since the acceptor has already been shut down.")
    } else {
      debug(s"Starting processors for listener ${endPoint.listenerName}")
      started = true
      processors.foreach(_.start())
      debug(s"Starting acceptor thread for listener ${endPoint.listenerName}")
      thread.start()
    }
  })


  def enableRequestProcessing(
                             ): Unit = this.synchronized {
    if (socketServer.stopped) {
      throw new RuntimeException("Can't enable request processing: SocketServer is stopped.")
    }
    info("Enabling request processing.")
    startFuture.complete(null);
  }

  private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
    override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
  }

  private[network] def removeProcessors(removeCount: Int): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.close())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  def beginShutdown(): Unit = {
    if (shouldRun.getAndSet(false)) {
      wakeup()
      synchronized {
        processors.foreach(_.beginShutdown())
      }
    }
  }

  def close(): Unit = {
    beginShutdown()
    thread.join()
    synchronized {
      processors.foreach(_.close())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  override def run(): Unit = {
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    try {
      while (shouldRun.get()) {
        try {
          acceptNewConnections()
          closeThrottledConnections()
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket, selector, and any throttled sockets.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket, this))
      throttledSockets.clear()
    }
  }

  /**
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int, listenBacklogSize: Int): ServerSocketChannel = {
    val socketAddress =
      if (Utils.isBlank(host))
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress, listenBacklogSize)
      info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
    }
    serverChannel
  }

  /**
   * Listen for new connections and assign accepted connections to processors using round-robin.
   */
  private def acceptNewConnections(): Unit = {
    //这个nioSelector 只关注OP_ACCEPT事件，然后将新的连接交给Processor处理
    val ready = nioSelector.select(500)
    if (ready > 0) {
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      while (iter.hasNext && shouldRun.get()) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            accept(key).foreach { socketChannel =>
              // Assign the channel to the next processor (using round-robin) to which the
              // channel can be added without blocking. If newConnections queue is full on
              // all processors, block until the last one is able to accept a connection.
              var retriesLeft = synchronized(processors.length)
              var processor: Processor = null

              def findProcessor = {
                retriesLeft -= 1
                processor = synchronized {
                  // adjust the index (if necessary) and retrieve the processor atomically for
                  // correct behaviour in case the number of processors is reduced dynamically
                  currentProcessorIndex = currentProcessorIndex % processors.length
                  processors(currentProcessorIndex)
                }
                currentProcessorIndex += 1
              }

              findProcessor
              while (!assignNewConnection(socketChannel, processor, retriesLeft == 0)) {
                findProcessor
              }
            }
          } else
            throw new IllegalStateException("Unrecognized key state for acceptor thread.")
        } catch {
          case e: Throwable => error("Error while accepting connection", e)
        }
      }
    }
  }

  /**
   * Accept a new connection
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      configureAcceptedSocketChannel(socketChannel)
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        connectionQuotas.closeChannel(this, endPoint.listenerName, socketChannel)
        None
      case e: ConnectionThrottledException =>
        val ip = socketChannel.socket.getInetAddress
        debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
        val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
        throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
        None
      case e: IOException =>
        error(s"Encountered an error while configuring the connection, closing it.", e)
        connectionQuotas.closeChannel(this, endPoint.listenerName, socketChannel)
        None
    }
  }

  protected def configureAcceptedSocketChannel(socketChannel: SocketChannel): Unit = {
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    socketChannel.socket().setKeepAlive(true)
    if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      socketChannel.socket().setSendBufferSize(sendBufferSize)
  }

  /**
   * Close sockets for any connections that have been throttled.
   */
  private def closeThrottledConnections(): Unit = {
    val timeMs = time.milliseconds
    while (throttledSockets.headOption.exists(_.endThrottleTimeMs < timeMs)) {
      val closingSocket = throttledSockets.dequeue()
      debug(s"Closing socket from ip ${closingSocket.socket.getRemoteAddress}")
      closeSocket(closingSocket.socket, this)
    }
  }

  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {
    if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
      debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

  /**
   * Wakeup the thread for selection.
   */
  def wakeup(): Unit = nioSelector.wakeup()

  def addProcessors(toCreate: Int): Unit = synchronized {
    val listenerName = endPoint.listenerName
    val securityProtocol = endPoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()

    for (_ <- 0 until toCreate) {
      val processor = newProcessor(socketServer.getNextProcessorId(), listenerName, securityProtocol)
      listenerProcessors += processor
      requestChannel.addProcessor(processor)

      if (started) {
        processor.start()
      }
    }
    processors ++= listenerProcessors
  }

  def newProcessor(id: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol): Processor = {
    val name = s"${threadPrefix()}-kafka-network-thread-${endPoint.listenerName}-${endPoint.securityProtocol}-${id}"
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext,
      Processor.ConnectionQueueSize,
      isPrivilegedListener,
      apiVersionManager,
      name)
  }
}

object SampleAcceptor {
  val ThreadPrefix = "simple_acceptor"
  val MetricPrefix = ""
}

class SampleAcceptor(socketServer: SocketServer,
                     endPoint: EndPoint,
                     config: KafkaConfig,
                     connectionQuotas: ConnectionQuotas,
                     time: Time,
                     isPrivilegedListener: Boolean,
                     requestChannel: RequestChannel,
                     metrics: Metrics,
                     credentialProvider: CredentialProvider,
                     logContext: LogContext,
                     memoryPool: MemoryPool,
                     apiVersionManager: ApiVersionManager) extends Acceptor(
  socketServer, endPoint, config, connectionQuotas, time, isPrivilegedListener, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager
) {
  override def metricPrefix(): String = SampleAcceptor.MetricPrefix

  override def threadPrefix(): String = SampleAcceptor.ThreadPrefix

  def configure(configs: util.Map[String, _]): Unit = {
    addProcessors(configs.get(KafkaConfig.NumNetworkThreadsProp).asInstanceOf[Int])
  }
}

