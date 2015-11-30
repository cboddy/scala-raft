package im.boddy.raft

import scala.collection.mutable
import scala.util.Random
import im.boddy.raft.Peer._

class PendingRequest(val id: RequestId, val index: Index, var nSucceeded : Int, var nFailed: Int)

class LeaderState[T](peer: Peer[T]) {

  val matchIndex = new mutable.HashMap[Id, Index]()
  var lastTimePing = NO_PING_SENT

  private val pending = new mutable.HashMap[Id, PendingRequest]()

  def reset = {
    matchIndex.clear()
    peer.config.peers.foreach(matchIndex.put(_, peer.commitIndex))
  }

  def updateCommitIndex: Unit = {
    val sorted = matchIndex.values.toSeq.sorted
    val majority: Index = sorted(peer.config.majority)
    peer.commitIndex = Math.max(peer.commitIndex, majority)
  }

  def handleSuccess(id: Id, entry: Entry): Unit = {
    val index = entry.index
    matchIndex.put(id, index)
    updateCommitIndex
    if (index < peer.lastApplied.index) {
      val entries = peer.getEntries(index +1, peer.lastApplied.index+1)
      peer.send(peer.addressedPDU(AppendEntries(peer.currentTerm, peer.id, entry, entries, peer.commitIndex), id))
    }
  }

  def handleMissing(id: Id, index: Index): Unit = {
    if (matchIndex.get(id).get >= index) {
      val previous: Entry = peer.getEntry(index - 1).id
      peer.send(peer.addressedPDU(AppendEntries(peer.currentTerm, peer.id, previous, Seq(), peer.commitIndex), id))
    }
  }


  def requestHasMajority(client: Id, request: RequestId, index: Index) : Boolean = {
    val maybe = pending.get(client)
    if (maybe.isDefined) {
      val req = maybe.get
      req.id == request && req.index == index && (req.nSucceeded >= peer.config.majority || req.nFailed >= peer.config.majority)
    }
    else false
  }

  def removePending(id: Id) = pending.remove(id)

  def containsPending(id: Id) = pending.contains(id)

  def addPending(id: Id, req: PendingRequest) = pending.put(id, req)

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
                       val timeoutSeed: Duration,
                       val random : Random = Random)extends Runnable with LogRepository[T] with Broker with Logging with AutoCloseable {

  if (! config.peers.contains(id)) throw new IllegalStateException("peer "+ id + " not in config " + config)

  private[raft] val leaderTimeout = timeoutSeed.copy(count = timeoutSeed.count/2)

  private[raft] var electionTimeout = nextElectionTimeout

  private[raft] val leaderState = new LeaderState(this)

  private[raft] val peerVoteResults : collection.mutable.Map[Id, Boolean] = collection.mutable.Map()

  private[raft] var commitIndex: Index = NO_TERM


  private[raft] var currentTerm: Term = NO_TERM
  private[raft] var votingTerm: Term = NO_TERM
  private[raft] var lastApplied : Entry  = Entry(NO_TERM, NO_TERM)


  private[raft] var leader : Id = NO_LEADER
  private[raft] var votedFor : Id = NOT_VOTED

  @volatile private[raft] var isFinished = false

  private[raft] var state = State.CANDIDATE

  private[raft] def peerTick : Unit = {
    val timeout = if (state == State.LEADER) leaderTimeout else electionTimeout

    val opt: Option[AddressedPDU] = receive(timeout)
    if (opt.nonEmpty) {
      val received = opt.get
      val source = received.source
      val pdu = received.pdu

      if (! config.peers.contains(source))
        send(addressedPDU(InvalidPDU(InvalidPduState.INVALID_ID, currentTerm), source))
      else {
        val handler: (AddressedPDU => Unit) = pdu match {
          case _: AppendEntries[T] => handleAppend
          case _: AppendEntriesAck => handleAppendAck
          case _: RequestVote => handleRequestVote
          case _: RequestVoteAck => handleRequestAck
          case _: ClientRequest[T] => handleClient
          case _ => throw new IllegalStateException("No handler for " + pdu)
        }
        handler(received)
      }
    }
    else if (state != State.LEADER) callElection

    if (state == State.LEADER) leaderTick
  }

  def run(): Unit = {
    log.info("peer "+ toString() +" starting")

    while (! isFinished) peerTick
  }

  def addressedPDU(pdu: PDU, target: Id) : AddressedPDU = AddressedPDU(id, target, pdu)

  def handleAppend(appendEntries: AddressedPDU) : Unit = {
    if (appendEntries.target != id)
      throw new IllegalStateException("PDU "+ appendEntries +" not intended for peer "+ id)

    val (source, pdu) = (appendEntries.source, appendEntries.pdu.asInstanceOf[AppendEntries[T]])

    val appendState = handleAppend(pdu, source)

    send(addressedPDU(AppendEntriesAck(currentTerm, appendState, lastApplied, commitIndex, leader), source))
  }

  def handleAppend(pdu: AppendEntries[T], source: Id) : AppendState.Value= {
    lazy val ensureFollower = () => {
      if (source != leader || state != State.FOLLOWER) {
        descendToFollower(pdu.term, source)
      }
    }

    pdu match {

      case _ if pdu.term < currentTerm => AppendState.TERM_NOT_CURRENT

      case _ if lastApplied != pdu.previous => {
        ensureFollower()
        AppendState.MISSING_ENTRIES
      }
      case _ => {
        ensureFollower()
        if (pdu.entries.nonEmpty) {
          putEntries(pdu.entries)
          lastApplied = pdu.entries.last.id
          commitIndex = Math.max(commitIndex, pdu.committedIndex)
        }
        AppendState.SUCCESS
      }
    }
  }

  def handleAppendAck(ack : AddressedPDU) = {
    val (source, pdu) = (ack.source, ack.pdu.asInstanceOf[AppendEntriesAck])
    pdu.state match {
      case AppendState.TERM_NOT_CURRENT => descendToFollower(pdu.term, pdu.leader)
      case AppendState.MISSING_ENTRIES => leaderState.handleMissing(source, pdu.previous.index)
      case AppendState.SUCCESS => leaderState.handleSuccess(source, pdu.previous)
    }
  }

  def handleRequestVote(requestVote: AddressedPDU) = {
    val (source, pdu) = (requestVote.source, requestVote.pdu.asInstanceOf[RequestVote])

    lazy val candidateLogOutOfDate = pdu.previous < lastApplied

    val voteState: RequestVoteState.Value = pdu match {

      case _ if pdu.term < currentTerm => {
        //received PDU with term less than peers
        RequestVoteState.TERM_NOT_CURRENT
      }

      case _ if candidateLogOutOfDate => RequestVoteState.LOG_OUT_OF_DATE

      case _ if pdu.term == currentTerm => votedFor match {
        //received PDU with term equal to  peer's
        case x if x == NOT_VOTED => {
          votedFor = source
          RequestVoteState.SUCCESS
        }
        case x if x == source => {
          RequestVoteState.SUCCESS
        }
        case _ => RequestVoteState.VOTE_ALREADY_CAST
      }
      case _ => {
        //received PDU with term in advance of peer's
        currentTerm = pdu.term

        state = State.FOLLOWER
        leader = NO_LEADER

        votedFor = source
        RequestVoteState.SUCCESS
      }
    }

    send(addressedPDU(RequestVoteAck(currentTerm, voteState, leader), source))
  }

  def handleRequestAck(ack: AddressedPDU): Unit = {
    val (source, pdu) = (ack.source, ack.pdu.asInstanceOf[RequestVoteAck])
    pdu.state match {
      case RequestVoteState.TERM_NOT_CURRENT => {
        descendToFollower(pdu.term, pdu.leader)
      }
      case RequestVoteState.VOTE_ALREADY_CAST => {
        addVote(source, false)
      }
      case RequestVoteState.SUCCESS => {
        addVote(source, true)
        if (peerVoteResults.values.count(_ == true) >= config.majority)
          ascendToLeader
      }
    }
  }

  def handleClient(req: AddressedPDU) = {
    val (source, pdu) = (req.source, req.pdu.asInstanceOf[ClientRequest[T]])
    state match {
      case State.LEADER => {
        val entry  = LogEntry(nextEntry, pdu.value)
        val append = AppendEntries(currentTerm, id, lastApplied, Seq(entry), commitIndex)
        broadcast(append, _ => true)
      }
      case _ => {
        val pdu = addressedPDU(ClientResponse(failure, leader), source)
        send(pdu)
      }
    }
  }

  def broadcast(pdu: PDU, filter: (Id) => Boolean = _ != id) = config.peers
    .filter(filter)
    .map(addressedPDU(pdu, _))
    .foreach(send)

  def ascendToLeader {

    log.info("peer " + this.toString() + " ascending to leader")

    state = State.LEADER
    leader = id
    leaderState.reset
  }

  def descendToFollower(withTerm: Term, withLeader: Id) {

    log.info("peer " + this.toString() + " descending to follower of leader " + withLeader + " with  term " + withTerm)

    state = State.FOLLOWER
    leader = withLeader
    currentTerm = withTerm
  }

  def callElection {

    log.info("peer " + this.toString() + " called election for term " + currentTerm)

    currentTerm += 1

    resetVotes
    addVote(id, true)

    val pdu = RequestVote(currentTerm, id, lastApplied)
    broadcast(pdu)

    electionTimeout = nextElectionTimeout
  }

  def leaderTick {
    val currentTime = now

    if (leaderState.updatePing(currentTime)) {
      leaderState.lastTimePing = currentTime
      broadcast(AppendEntries(currentTerm, leader, lastApplied, Seq(), commitIndex))
    }
  }

  def resetVotes: Unit = {
    votingTerm = currentTerm
    peerVoteResults.clear()
  }

  def addVote(id: Id, hasVote: Boolean) = peerVoteResults.put(id, hasVote)

  def nextEntry = lastApplied.copy(index = lastApplied.index+1)

  private[raft] def nextElectionTimeout = {
    val seed = timeoutSeed.count
    timeoutSeed.copy(count = seed + random.nextInt(seed))
  }

  override def toString() = id.toString

  def close = isFinished = true
}

object Peer {
  val failure: Entry = Entry(NO_TERM, NO_TERM)
}