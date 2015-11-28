package im.boddy.raft

abstract class PDU(term: Term)

case class AddressedPDU(source: Id, target: Id, pdu: PDU)

case class AppendEntries[T](term: Term,
                            leaderId: Id,
                            previousIndex: Index,
                            previousTerm: Term,
                            entries : Seq[LogEntry[T]],
                            leaderCommit: Index
                           ) extends PDU(term) {
  lazy val committedIndex = Math.min(leaderCommit, entries.last.id.index)
}

case class RequestVote(term: Term,
                       candidate: Id,
                       lastLogIndex: Index,
                       lastLogTerm: Index) extends PDU(term) {
  if (term < lastLogTerm) throw new IllegalStateException()
}


object AppendState extends Enumeration {
  val TERM_NOT_CURRENT, REQUEST_MISSING_ENTRIES, PEER_MISSING_ENTRIES, SUCCESS = Value
}

case class AppendEntriesAck(term: Term,
                            state: AppendState.Value,
                            previousIndex:  Index,
                            previousTerm: Term,
                            commitIndex: Index,
                            leader: Id) extends PDU(term) {
  def success = state == AppendState.SUCCESS
}


case object RequestVoteState extends Enumeration {
  val TERM_NOT_CURRENT, VOTE_ALREADY_CAST, SUCCESS = Value
}

case class RequestVoteAck(term: Term, state: RequestVoteState.Value, leader: Id) extends PDU(term) {
  if (leader != NO_LEADER && state == RequestVoteState.TERM_NOT_CURRENT)
    throw new IllegalStateException()

  def success = state == RequestVoteState.SUCCESS
}

case object InvalidPduState extends Enumeration {
  val INVALID_ID, INVALID_SOURCE = Value
}

case class InvalidPDU(state: InvalidPduState.Value, term: Term) extends PDU(term)
