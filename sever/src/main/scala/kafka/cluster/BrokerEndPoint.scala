package kafka.cluster

object BrokerEndPoint {

  private val uriParseExp = """\[?([0-9a-zA-Z\-%._:]*)\]?:([0-9]+)""".r

  /**
   * BrokerEndPoint URI is host:port or [ipv6_host]:port
   * Note that unlike EndPoint (or listener) this URI has no security information.
   */
  def parseHostPort(connectionString: String): Option[(String, Int)] = {
    connectionString match {
      case uriParseExp(host, port) => try Some(host, port.toInt) catch { case _: NumberFormatException => None }
      case _ => None
    }
  }
}

/**
 * BrokerEndpoint is used to connect to specific host:port pair.
 * It is typically used by clients (or brokers when connecting to other brokers)
 * and contains no information about the security protocol used on the connection.
 * Clients should know which security protocol to use from configuration.
 * This allows us to keep the wire protocol with the clients unchanged where the protocol is not needed.
 */
case class BrokerEndPoint(id: Int, host: String, port: Int) {
  override def toString: String = {
    s"BrokerEndPoint(id=$id, host=$host:$port)"
  }
}