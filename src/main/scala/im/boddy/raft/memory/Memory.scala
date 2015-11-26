package im.boddy.raft.memory

import java.util.concurrent._

import im.boddy.raft._

import scala.collection.mutable

import scala.collection.mutable.ArrayBuffer

class AsyncBroker[T] (config: Config, timeout: Duration) extends Logging {

  private val msgs = new mutable.HashMap[Id, BlockingQueue[AddressedPDU]]()

  private val threadPool = Executors.newFixedThreadPool(config.peers.size)

  def addPeer(id: Id, timeout: Duration) : Peer[T] = {

    val repo = new BufferLogRepository[T]()

    val peer = new Peer[T](id, config, timeout) {

      override def getEntries(startIndex: Index, endIndex: Index)  = repo.getEntries(startIndex, endIndex)

      override def putEntries(entries: Seq[LogEntry[T]]) = repo.putEntries(entries)

      override def send(pdu: AddressedPDU): Unit = offer(pdu)

      override def receive(timeout: Duration): Option[AddressedPDU] = {
        val head = poll(id, timeout)
        Option(head)
      }
    }

    msgs.put(peer.id, new ArrayBlockingQueue[AddressedPDU](1))
    threadPool.submit(peer)
    return peer
  }

  def offer(pdu: AddressedPDU): Unit = {
    val maybe: Option[BlockingQueue[AddressedPDU]] = msgs.get(pdu.target)
    if (maybe.isEmpty) throw new IllegalStateException("No peer with id "+ pdu.target)
    maybe.get.offer(pdu, timeout.count, timeout.unit)
  }

  def poll(id: Id, timeout: Duration): AddressedPDU = {
    val maybe: Option[BlockingQueue[AddressedPDU]] = msgs.get(id)
    if (maybe.isEmpty) throw new IllegalStateException("No peer with id "+ id)

    log.info("polling " + id +" with timeout "+ timeout)
    maybe.get.poll(timeout.count, timeout.unit)
  }
  
  def shutdown {
    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.MINUTES)
  }
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