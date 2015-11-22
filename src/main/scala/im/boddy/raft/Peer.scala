package im.boddy.raft

import java.util.concurrent.{TimeUnit, TimeoutException}

import scala.collection.mutable

class Leader[T] {
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

abstract class Broker {
  def send(pdu: SourcedPDU)
  def receive() : java.util.concurrent.Future[SourcedPDU]
}

abstract class Peer[T](val id: Id,
                       val repository: LogRepository[T],
                       val broker: Broker,
                       val timeout: Timeout) extends Runnable {

  implicit def toSent(pdu: PDU) : SourcedPDU = SourcedPDU(id, pdu)

  private var currentTerm: Term = 0
  private var lastCommittedIndex: Index  = 0
  private var lastAppliedIndex : Index  = 0
  private var lastAppliedTerm : Term  = 0

  private var leader : Id = -1

  @volatile var isFinished = false

  private val state = State.CANDIDATE

  def run(): Unit = {
    while (! isFinished) {
      try {
        val received: SourcedPDU = broker.receive.get(timeout, TimeUnit.MILLISECONDS)

        val handler: (SourcedPDU => Unit) = received.pdu match {
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

  def handleAppend(appendEntries: SourcedPDU) = {
    val (source, pdu) = (appendEntries.source, appendEntries.pdu.asInstanceOf[AppendEntries[T]])

    val appendState: AppendState.Value = pdu match {
      case _ if pdu.term < currentTerm => AppendState.TERM_NOT_CURRENT
      case _ if lastAppliedIndex != pdu.previousIndex || lastAppliedTerm != pdu.previousTerm => AppendState.MISSING_PREVIOUS_ENTRY
      case _ => {
        if (pdu.entries.nonEmpty) {
          repository.putEntries(pdu.entries)
          lastCommittedIndex = Math.max(lastCommittedIndex, pdu.committedIndex)
        }
        AppendState.SUCCESS
      }
    }

    broker.send(AppendEntriesAck(currentTerm, appendState))
  }

  def handleAppendAck(ack : SourcedPDU) = {
    val (source, pdu) = (ack.source, ack.pdu.asInstanceOf[AppendEntriesAck])
  }

  def handleRequestVote(requestVote: SourcedPDU) = {
    val (source, pdu) = (requestVote.source, requestVote.pdu.asInstanceOf[RequestVote])

  }

  def handleRequestAck(ack: SourcedPDU) = {
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

