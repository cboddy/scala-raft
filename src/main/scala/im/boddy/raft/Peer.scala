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

case class Config(peers: Seq[Id])

case object State extends Enumeration {
  val FOLLOWER, CANDIDATE, LEADER = values
}

trait Broker {
  def send(pdu: AddressedPDU)
  def receive(timeout: Timeout) : java.util.concurrent.Future[AddressedPDU]
}

abstract class Peer[T](val id: Id,
                       val config:  Config,
                       val timeout: Timeout) extends Runnable with LogRepository[T] with Broker with Logging {

  if (! config.peers.contains(id)) throw new IllegalStateException("peer "+ id + " not in config " + config)

  private val leaderState = new LeaderState
  private var currentTerm: Term = NO_TERM
  private var votingTerm: Term = NO_TERM
  private var lastCommittedIndex: Index = NO_TERM
  private var lastAppliedIndex : Index  = NO_TERM
  private var lastAppliedTerm : Term  = NO_TERM

  private var leader : Id = NO_LEADER
  private var votedFor : Id = NOT_VOTED

  @volatile var isFinished = false

  private var state = State.CANDIDATE

  def run(): Unit = {
    while (! isFinished) {
      try {
        val received: AddressedPDU = receive(timeout).get()

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

  def addressedPDU(pdu: PDU, target: Id) : AddressedPDU = AddressedPDU(id, target, pdu)

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

    send(addressedPDU(AppendEntriesAck(currentTerm, appendState, lastAppliedIndex, lastAppliedTerm), source))
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

  def handleTimeout = callElection

  def broadcast(pdu: PDU) = config.peers
    .filterNot(_ == id)
    .map(addressedPDU(pdu, _))
    .foreach(send)


  def ascendToLeader {
    lazy val msg = "peer " + this.toString() + " ascending to leader"
    log.fine(msg)

    state = State.LEADER
    leader = id
    leaderState.reset
    config.peers.filter(_ != id).foreach(leaderState.matchIndex.put(_, lastCommittedIndex))
  }

  def descendToFollower(withTerm: Term, withLeader: Id) {
    lazy val msg = "peer " + this.toString() + " descending to follower of leader " + withLeader + " with  term " + withTerm
    log.fine(msg)

    state = State.FOLLOWER
    leader = withLeader
    currentTerm = withTerm
  }

  def callElection {
    lazy val msg = "peer "+ this.toString() +" called election for term "+ currentTerm
    log.fine(msg)

    if (shouldIncrementTerm) {
      currentTerm += 1
      resetVotes
    }

    addVote(id, true)

    val pdu = RequestVote(currentTerm, id, lastAppliedIndex, lastAppliedTerm)
    broadcast(pdu)
  }

  def shouldIncrementTerm = ???
  def resetVotes = ???
  def addVote(id: Id, vote: Boolean) = ???

  override def toString() = id.toString
}