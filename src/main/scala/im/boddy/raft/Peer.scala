package im.boddy.raft

import scala.collection.mutable

class PendingRequest(val id: RequestId, val index: Index, var nSucceeded : Int, var nFailed: Int)

class LeaderState[T](peer: Peer[T]) {
  val nextIndex = new mutable.HashMap[Id, Index]()
  val matchIndex = new mutable.HashMap[Id, Index]()
  var lastTimePing = NO_PING_SENT

  private val pending = new mutable.HashMap[ClientId, PendingRequest]()
  private val groupSize = peer.config.peers.length

  def reset = {
    nextIndex.clear()
    matchIndex.clear()
  }

  def requestHasMajority(client: ClientId, request: RequestId, index: Index) : Boolean = {
    val maybe = pending.get(client)
    if (maybe.isDefined) {
      val req = maybe.get
      req.id == request && req.index == index && (req.nSucceeded >= groupSize/2 || req.nFailed >= groupSize/2)
    }
    else false
  }

  def removePending(id: ClientId) = pending.remove(id)

  def containsPending(id: ClientId) = pending.contains(id)

  def addPending(id: ClientId, req: PendingRequest) = pending.put(id, req)

  def updatePing(time: Long) : Boolean = {
    val deltaTime = time - lastTimePing
    peer.leaderTimeout.toMillis < deltaTime
  }

}

case object State extends Enumeration {
  val FOLLOWER, CANDIDATE, LEADER = Value
}


abstract class Peer[T](val id: Id,
                       val config:  Config,
                       val electionTimeout: Duration) extends Runnable with LogRepository[T] with Broker with Logging with AutoCloseable {

  if (! config.peers.contains(id)) throw new IllegalStateException("peer "+ id + " not in config " + config)

  val leaderTimeout = electionTimeout.copy(count = electionTimeout.count/2)

  private val leaderState = new LeaderState(this)

  private val peerVoteResults = collection.mutable.Map() ++ config.peers.map(_ -> NOT_VOTED).toMap

  private var currentTerm: Term = NO_TERM
  private var votingTerm: Term = NO_TERM
  private var lastCommittedIndex: Index = NO_TERM
  private var lastAppliedIndex : Index  = NO_TERM
  private var lastAppliedTerm : Term  = NO_TERM

  private var leader : Id = NO_LEADER
  private var votedFor : Id = NOT_VOTED

  @volatile var isFinished = false

  private var state = State.CANDIDATE

  def tick : Unit = {
    val timeout = if (state == State.LEADER) leaderTimeout else electionTimeout

    val opt: Option[AddressedPDU] = receive(timeout)
    if (opt.nonEmpty) {
      val received = opt.get
      val source = received.source
      val pdu = received.pdu

      if (! config.peers.contains(pdu))
        send(addressedPDU(InvalidPDU(InvalidPduState.INVALID_ID, currentTerm), source))
      else {
        val handler: (AddressedPDU => Unit) = pdu match {
          case _: AppendEntries[T] => handleAppend
          case _: AppendEntriesAck => handleAppendAck
          case _: RequestVote => handleRequestVote
          case _: RequestVoteAck => handleRequestAck
          case _ => throw new IllegalStateException("No handler for " + pdu)
        }
        handler(received)
      }
    }
    else if (state != State.LEADER) callElection

    if (state == State.LEADER) leaderPing
  }

  def run(): Unit = {
    log.info("peer "+ toString() +" starting")

    while (! isFinished) tick
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

    lazy val follow = () => {
      descendToFollower(pdu.term, source)
      RequestVoteState.SUCCESS
    }

    val voteState: RequestVoteState.Value = pdu match {

      case _ if pdu.term < currentTerm => RequestVoteState.TERM_NOT_CURRENT
      case _ if lastAppliedIndex > pdu.lastLogIndex || lastAppliedTerm > pdu.lastLogTerm => RequestVoteState.CANDIDATE_MISSING_PREVIOUS_ENTRY
      case _ if pdu.term == currentTerm => votedFor match {
          case x if x == NOT_VOTED => follow()
          case x if x == source => follow()
          case _ => RequestVoteState.VOTE_ALREADY_CAST
      }
      case _ => follow()
    }

    send(addressedPDU(RequestVoteAck(currentTerm, voteState), source))
  }

  def handleRequestAck(ack: AddressedPDU) = {
    val (source, pdu) = (ack.source, ack.pdu.asInstanceOf[RequestVoteAck])
  }

  def broadcast(pdu: PDU) = config.peers
    .filterNot(_ == id)
    .map(addressedPDU(pdu, _))
    .foreach(send)

  def ascendToLeader {

    log.info("peer " + this.toString() + " ascending to leader")

    state = State.LEADER
    leader = id
    leaderState.reset
    config.peers.filterNot(_ == id).foreach(leaderState.matchIndex.put(_, lastCommittedIndex))
  }

  def descendToFollower(withTerm: Term, withLeader: Id) {

    log.info("peer " + this.toString() + " descending to follower of leader " + withLeader + " with  term " + withTerm)

    state = State.FOLLOWER
    leader = withLeader
    currentTerm = withTerm
  }

  def callElection {

    log.info("peer " + this.toString() + " called election for term " + currentTerm)

    if (shouldIncrementTerm) {
      currentTerm += 1

    }
    resetVotes
    addVote(id, true)

    val pdu = RequestVote(currentTerm, id, lastAppliedIndex, lastAppliedTerm)
    broadcast(pdu)
  }

  def leaderPing {
    val currentTime = now

    if (leaderState.updatePing(currentTime)) {
      leaderState.lastTimePing = currentTime
      broadcast(AppendEntries(currentTerm, leader, lastAppliedIndex, lastAppliedTerm, Seq(), lastCommittedIndex))
    }
  }

  def shouldIncrementTerm = {
    if (votingTerm != currentTerm) true
    else if (peerVoteResults.exists(_ == NOT_VOTED)) false
    else true
  }

  def resetVotes: Unit = {
    votingTerm = currentTerm
    config.peers.foreach(peerVoteResults.put(_, NOT_VOTED))
  }

  def addVote(id: Id, vote: Boolean) = peerVoteResults.put(id, id)

  override def toString() = id.toString

  def close {
    isFinished = true
  }
}
