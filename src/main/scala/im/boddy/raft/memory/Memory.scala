package im.boddy.raft.memory


import im.boddy.raft._
import java.util.concurrent._

import scala.collection.mutable
import AsyncBroker._

import scala.collection.mutable.ArrayBuffer

class AsyncBroker[T] extends Broker {

  private val msgs = new mutable.HashMap[Id, BlockingQueue[AddressedPDU]]()
  
  private val threadPool = Executors.newCachedThreadPool()
  
  def addPeer(peer: Peer[T]) = msgs.put(peer.id, new ArrayBlockingQueue[AddressedPDU](1))

  override def send(pdu: AddressedPDU): Unit = {
    val maybe: Option[BlockingQueue[AddressedPDU]] = msgs.get(pdu.target)
    if (maybe.isEmpty) throw new IllegalStateException("No peer with id "+ pdu.target)
    maybe.get.offer(pdu, TIMEOUT, MILLIS)
  }

  override def receive(id: Id, timeout: Timeout): Future[AddressedPDU] = {
    val maybe: Option[BlockingQueue[AddressedPDU]] = msgs.get(id)
    if (maybe.isEmpty) throw new IllegalStateException("No peer with id "+ id)

    val callable: Callable[AddressedPDU] = new  Callable[AddressedPDU] {
      override def call(): AddressedPDU = maybe.get.poll(timeout, MILLIS)
    }
    threadPool.submit(callable)
  }
  
  def shutdown {
    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.MINUTES)
  }
}

object AsyncBroker {
  val TIMEOUT:  Timeout = 500
  val MILLIS = TimeUnit.MILLISECONDS
}


class BufferLogRepository[T] extends LogRepository[T] {
  
  val log = new ArrayBuffer[LogEntry[T]]()
  
  override def getEntries(startIndex: Index, endIndex: Index): Seq[LogEntry[T]] = {
    val length: Int = log.size
    if (length < endIndex) throw new IllegalStateException("Requested index "+ endIndex +" past limit "+ length)
    log.slice(startIndex.toInt, endIndex.toInt)
  }

  override def putEntries(entries: Seq[LogEntry[T]]) = log ++= entries
}