package im.boddy.raft

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.logging.Level

import org.specs2.mutable._

class MemorySpec extends Specification with Logging {

  class TestSystem[T](val config: Config, val timeout: Duration) {

    val repo = new BufferLogRepository[T]()
    val toPeerMsgs = new ArrayBlockingQueue[AddressedPDU](16)
    val fromPeerMsgs = new ArrayBlockingQueue[AddressedPDU](16)

    trait TestRepo extends LogRepository[T] {
      override def getEntries(start: Index, end: Index)  = repo.getEntries(start, end)
      override def putEntries(entries: Seq[LogEntry[T]]) {repo.putEntries(entries)}
      override def containsEntry(entryKey: Entry) = repo.containsEntry(entryKey)
    }

    trait TestBroker extends Broker {
      def send(pdu: AddressedPDU) = fromPeerMsgs.add(pdu)
      def receive(timeout: Duration) : Option[AddressedPDU] = Option(toPeerMsgs.poll(timeout.count, timeout.unit))
    }

    val peerId = config.peers.head

    val peer = new Peer[T](peerId, config, timeout) with TestRepo with TestBroker
  }

  log.setLevel(Level.FINEST)

  def testSystem(config : Config = Config(Seq(1,2,3)), timeout : Duration = Duration(1, TimeUnit.SECONDS))(pdus : AddressedPDU*) = {
    val system = new TestSystem[Int](config, timeout)
    pdus.foreach(system.toPeerMsgs.offer(_))
    Tuple4[ArrayBlockingQueue[AddressedPDU], ArrayBlockingQueue[AddressedPDU], Peer[Int], TestSystem[Int]](system.toPeerMsgs, system.fromPeerMsgs, system.peer, system)
  }

  "Peer" should {

    "handle invalid PDU" in {

      val (out, in, peer, system) = testSystem()(AddressedPDU(-1, 1, RequestVote(-1, 2, Entry(0, -1))))
      peer.peerTick
      in.size() mustEqual(1)
      val response : AddressedPDU = in.take()
      val responsePdu: PDU = response.pdu
      responsePdu.isInstanceOf[InvalidPDU] mustEqual true
      val invalidPdu: InvalidPDU = responsePdu.asInstanceOf[InvalidPDU]
      invalidPdu.state mustEqual InvalidPduState.INVALID_ID
      invalidPdu.term mustEqual NO_TERM
    }

    "reject vote with invalid term" in {
      val (out, in, peer, system) = testSystem()()
      peer.currentTerm = 3

      out.put(AddressedPDU(2, 1, RequestVote(2, 2, Entry(0, 0))))
      peer.peerTick
      in.size() mustEqual(1)
      val response = in.take()
      response.pdu.isInstanceOf[RequestVoteAck] mustEqual true
      val ack = response.pdu.asInstanceOf[RequestVoteAck]
      ack.state mustEqual RequestVoteState.TERM_NOT_CURRENT
    }

//    "reject vote with not up to date previous index/term" in {
//      val (out, in, peer, system) = testSystem()()
//      peer.lastAppliedIndex = 10
//      peer.lastAppliedTerm = 3
//
//      for (pdu <- Seq(RequestVote(2, 2, 10, 2), RequestVote(3, 2, 9, 3))) {
//        val addressed = AddressedPDU(2, 1, pdu)
//        out.put(addressed)
//        peer.peerTick
//        in.size() mustEqual(1)
//        val response = in.take()
//        response.pdu.isInstanceOf[RequestVoteAck] mustEqual true
//        val ack = response.pdu.asInstanceOf[RequestVoteAck]
//        ack.state mustEqual RequestVoteState.CANDIDATE_MISSING_PREVIOUS_ENTRY
//      }
//      ok
//    }

    "grant vote to valid request-for-vote" in {
      val (out, in, peer, system) = testSystem()()
      peer.currentTerm = 3
      peer.lastApplied = Entry(10,3)

      for (pdu <- Seq(RequestVote(3, 2, Entry(10, 3)))) {
        val addressed = AddressedPDU(2, 1, pdu)
        out.put(addressed)
        peer.peerTick
        in.size() mustEqual(1)
        val response = in.take()

        response.pdu.isInstanceOf[RequestVoteAck] mustEqual true
        val ack = response.pdu.asInstanceOf[RequestVoteAck]
        ack.state mustEqual RequestVoteState.SUCCESS
        peer.currentTerm mustEqual pdu.term
        peer.leader = addressed.source
      }
      ok
    }

    "reject vote request if vote already cast and grant vote with term and index in advance of its own" in {
      val (out, in, peer, system) = testSystem()()
      peer.currentTerm = 3
      peer.lastApplied = Entry(10,3)
      peer.votedFor = 3
      peer.leader = 3


      for ((pdu, state) <- Seq(
        RequestVote(3, 2, Entry(10, 3)) -> RequestVoteState.VOTE_ALREADY_CAST,
        RequestVote(4, 2, Entry(16, 4)) -> RequestVoteState.SUCCESS)
      ) {
        val addressed = AddressedPDU(2, 1, pdu)
        out.put(addressed)
        peer.peerTick
        in.size() mustEqual(1)
        val response = in.take()
        response.pdu.isInstanceOf[RequestVoteAck] mustEqual true
        val ack = response.pdu.asInstanceOf[RequestVoteAck]
        ack.state mustEqual state
      }
      ok
    }


    "call election after timeout" in {
      val (out, in, peer, system) = testSystem()()
      peer.lastApplied= peer.lastApplied.copy(index=10)
      peer.peerTick

      peer.state mustEqual State.CANDIDATE
      peer.currentTerm mustNotEqual NO_TERM

      in.size mustEqual system.config.peers.size -1

      val response = in.take()
      response.pdu.isInstanceOf[RequestVote] mustEqual true

      val req = response.pdu.asInstanceOf[RequestVote]

      req.candidate mustEqual peer.id
      req.previous mustEqual peer.lastApplied
      req.term mustEqual peer.currentTerm
    }

    "reject append-entries with term out of date" in {
      val (out, in, peer, system) = testSystem()()
      peer.currentTerm = 0
      peer.lastApplied = peer.lastApplied.copy(term=0)
      peer.leader = 3

      val pdu = AppendEntries(peer.currentTerm-1, peer.leader, peer.lastApplied, Seq(), peer.lastApplied.index)

      out.add(AddressedPDU(2, peer.id, pdu))
      peer.peerTick

      in.size mustEqual 1

      val response = in.take()

      response.pdu.isInstanceOf[AppendEntriesAck] mustEqual true
      val ack = response.pdu.asInstanceOf[AppendEntriesAck]

      ack.state mustEqual AppendState.TERM_NOT_CURRENT
      ack.previous mustEqual peer.lastApplied

  }

  "reject append-entries if previous-index and previous-term don't match it's own" in {
    ???
    ok
  }

  "overwrite conflicting un-committed log entries" in {
    ???
    ok
  }

  "update state after received append-ack" in {
    ???
    ok
  }

}
}
