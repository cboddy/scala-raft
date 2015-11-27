package im.boddy.raft

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.logging.Level

import im.boddy.raft._
import org.specs2.mutable._
import org.specs2.specification.AfterAll

class MemorySpec extends Specification with Logging {


  class TestSystem[T](config: Config, timeout: Duration) {

    val repo = new BufferLogRepository[T]()
    val toPeerMsgs = new ArrayBlockingQueue[AddressedPDU](1)
    val fromPeerMsgs = new ArrayBlockingQueue[AddressedPDU](1)

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

  def testSystem = new TestSystem[Int](Config(Seq(1,2,3)), Duration(1, TimeUnit.SECONDS))

  log.setLevel(Level.FINEST)


  "Memory Test" should {

    val system = testSystem
    val (peer, out, in, timeout)  = (system.peer, system.toPeerMsgs, system.fromPeerMsgs, system.peer.electionTimeout)

    "handle invalid PDU" in {
      out.add(AddressedPDU(-1, 1, RequestVote(-1, 2, 0, 0)))
      peer.tick
      in.size() mustEqual(1)
      val response : AddressedPDU = in.take()
      val responsePdu: PDU = response.pdu
      responsePdu.isInstanceOf[InvalidPDU] mustEqual(true)
      val invalidPdu: InvalidPDU = responsePdu.asInstanceOf[InvalidPDU]
      invalidPdu.state mustEqual(InvalidPduState.INVALID_ID)
      invalidPdu.term mustEqual(NO_TERM)
    }
  }
}
