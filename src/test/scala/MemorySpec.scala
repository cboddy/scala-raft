package im.boddy.raft

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.logging.Level

import org.specs2.mutable._

class MemorySpec extends Specification with Logging {

  class TestSystem[T](config: Config, timeout: Duration) {

    val repo = new BufferLogRepository[T]()
    val toPeerMsgs = new ArrayBlockingQueue[AddressedPDU](16)
    val fromPeerMsgs = new ArrayBlockingQueue[AddressedPDU](16)

    trait TestRepo extends LogRepository[T] {
      override def getEntries(startIndex: Index, endIndex: Index)  = repo.getEntries(startIndex, endIndex)
      override def putEntries(entries: Seq[LogEntry[T]]) {repo.putEntries(entries)}
    }

    trait TestBroker extends Broker{
      def send(pdu: AddressedPDU) = fromPeerMsgs.add(pdu)
      def receive(timeout: Duration) : Option[AddressedPDU] = Option(toPeerMsgs.poll(timeout.count, timeout.unit))
    }

    val peerId = config.peers.head

    val peer = new Peer[T](peerId, config, timeout) with TestRepo with TestBroker
  }

  log.setLevel(Level.FINEST)

  def testSystem(pdus: AddressedPDU*) = {
    val system = new TestSystem[Int](Config(Seq(1,2,3)), Duration(1, TimeUnit.SECONDS))
    pdus.foreach(system.toPeerMsgs.offer(_))
    system
  }

  "Memory Test" should {

    "handle invalid PDU" in {
      val system = testSystem(AddressedPDU(-1, 1, RequestVote(-1, 2, 0, 0)))
      system.peer.tick
      val in = system.fromPeerMsgs
      in.size() mustEqual(1)
      val response : AddressedPDU = in.take()
      val responsePdu: PDU = response.pdu
      responsePdu.isInstanceOf[InvalidPDU] mustEqual(true)
      val invalidPdu: InvalidPDU = responsePdu.asInstanceOf[InvalidPDU]
      invalidPdu.state mustEqual(InvalidPduState.INVALID_ID)
      invalidPdu.term mustEqual(NO_TERM)
    }

    "reject vote with invalid term" in {
      val system = testSystem(AddressedPDU(-1, 1, RequestVote(-1, 2, 0, 0)))
      ok
    }
  }
}
