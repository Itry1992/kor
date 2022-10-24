package kafka.server

import com.tong.kafka.common.utils.BufferSupplier
object RequestLocal {
  val NoCaching: RequestLocal = RequestLocal(BufferSupplier.NO_CACHING)

  /** The returned instance should be confined to a single thread. */
  def withThreadConfinedCaching: RequestLocal = RequestLocal(BufferSupplier.create())
}

/**
 * Container for stateful instances where the lifecycle is scoped to one request.
 *
 * When each request is handled by one thread, efficient data structures with no locking or atomic operations
 * can be used (see RequestLocal.withThreadConfinedCaching).
 */
case class RequestLocal(bufferSupplier: BufferSupplier) {
  def close(): Unit = bufferSupplier.close()
}
