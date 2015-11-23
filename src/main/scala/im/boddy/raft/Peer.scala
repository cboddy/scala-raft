package im.boddy.raft

import java.util.concurrent.{TimeUnit, TimeoutException}

import scala.collection.mutable

class LeaderState {
  val nextIndex: collection.mutable.Map[Id, Index] = new mutable.HashMap[Id, Index]()
  val matchIndex: collection.mutable.Map[Id, Index] = new mutable.HashMap[Id, Index]()

  def reset = {
    nextIndex.clear()
    matchIndex.clear()
  }

}

case object State extends Enumeration {
  val FOLLOWER, CANDIDATE, LEADER = values
}

trait Broker {
  def send(pdu: AddressedPDU)
  def receive(id: Id, timeout: Timeout) : java.util.concurrent.Future[AddressedPDU]
}

abstract class Peer[T](val id: Id,
                       val broker: Broker,
                       val timeout: Timeout) extends Runnable with LogRepository[T]  {

  def addressedPDU(pdu: PDU, target: Id) : AddressedPDU = AddressedPDU(id, target, pdu)

  private var currentTerm: Term = 0
  private var lastCommittedIndex: Index = 0
  private var lastAppliedIndex : Index  = 0
  private var lastAppliedTerm : Term  = 0

  private var leader : Id = NO_LEADER
  private var votedFor : Id = NOT_VOTED

  @volatile var isFinished = false

  private val state = State.CANDIDATE

  def run(): Unit = {
    while (! isFinished) {
      try {
        val received: AddressedPDU = broker.receive(id, timeout).get()

        val handler: (AddressedPDU => Unit) = received.pdu match {
          case _ : AppendEntries[T] => handleAppend
          case _ : AppendEntriesAck => handleAppendAck
          case _ : RequestVote => handleRequestVote
          case _ : RequestVoteAck => handleRequestAck
          case _ => throw new IllegalStateException("No handler for " + received)
        }

        handler(received)
      } catch {
        case _ : TimeoutException => handleTimeout
      }
    }
  }

  def handleAppend(appendEntries: AddressedPDU) = {
    if (appendEntries.target != id)
      throw new IllegalStateException("PDU "+ appendEntries +" not intended for peer "+ id)

    val (source, pdu) = (appendEntries.source, appendEntries.pdu.asInstanceOf[AppendEntries[T]])

    val appendState: AppendState.Value = pdu match {
      case _ if pdu.term < currentTerm => AppendState.TERM_NOT_CURRENT
      case _ if lastAppliedIndex != pdu.previousIndex || lastAppliedTerm != pdu.previousTerm => AppendState.MISSING_PREVIOUS_ENTRY
      case _ if source != leader => throw new IllegalStateException()
      case _ => {
        if (pdu.entries.nonEmpty) {
          putEntries(pdu.entries)
          lastCommittedIndex = Math.max(lastCommittedIndex, pdu.committedIndex)
        }
        AppendState.SUCCESS
      }
    }

    broker.send(addressedPDU(AppendEntriesAck(currentTerm, appendState, lastAppliedIndex, lastAppliedTerm), source))
  }

  def handleAppendAck(ack : AddressedPDU) = {
    val (source, pdu) = (ack.source, ack.pdu.asInstanceOf[AppendEntriesAck])

  }

  def handleRequestVote(requestVote: AddressedPDU) = {
    val (source, pdu) = (requestVote.source, requestVote.pdu.asInstanceOf[RequestVote])

    val voteState: RequestVoteState.Value = pdu match {
      case _ if pdu.term < currentTerm => RequestVoteState.TERM_NOT_CURRENT
      case _ if lastAppliedIndex > pdu.lastLogIndex || lastAppliedTerm > pdu.lastLogTerm => RequestVoteState.CANDIDATE_MISSING_PREVIOUS_ENTRY
//      case _ if votedFor != NOT_VOTED  && votedFor != source => RequestVoteState
    }
  }

  def handleRequestAck(ack: AddressedPDU) = {
    val (source, pdu) = (ack.source, ack.pdu.asInstanceOf[RequestVoteAck])
  }

  def handleTimeout = {

  }

  def ascendToLeader: Unit = {

  }

  def descendToFollower(term : Term, leader: Id): Unit = {

  }

  def callElection: Unit = {

  }
}

