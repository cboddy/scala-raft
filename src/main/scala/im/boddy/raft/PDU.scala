package im.boddy.raft

sealed abstract class PDU

case class AddressedPDU(source: Id, target: Id, pdu: PDU)

case class AppendEntries[T](term: Term,
                            leaderId: Id,
                            previous: Entry,
                            entries : Seq[LogEntry[T]],
                            leaderCommit: Index
                           ) extends PDU {
  lazy val committedIndex = Math.min(leaderCommit, entries.last.id.index)
}

case class RequestVote(term: Term,
                       candidate: Id,
                       previous: Entry) extends PDU {
  if (term < previous.term) throw new IllegalStateException()
}


object AppendState extends Enumeration {
  val TERM_NOT_CURRENT, MISSING_ENTRIES, SUCCESS = Value
}

case class AppendEntriesAck(term: Term,
                            state: AppendState.Value,
                            previous: Entry,
                            commitIndex: Index,
                            leader: Id) extends PDU {
  def success = state == AppendState.SUCCESS
}


case object RequestVoteState extends Enumeration {
  val TERM_NOT_CURRENT, VOTE_ALREADY_CAST, LOG_OUT_OF_DATE, SUCCESS = Value
}

case class RequestVoteAck(term: Term, state: RequestVoteState.Value, leader: Id) extends PDU {
  if (leader != NO_LEADER && state == RequestVoteState.TERM_NOT_CURRENT)
    throw new IllegalStateException()

  def success = state == RequestVoteState.SUCCESS
}

case object InvalidPduState extends Enumeration {
  val INVALID_ID, INVALID_SOURCE = Value
}

case class InvalidPDU(state: InvalidPduState.Value, term: Term) extends PDU

case class ClientRequest[T](client: Id, request: RequestId, value: T) extends PDU

case class ClientResponse[T](entry: Entry, leader: Id) extends PDU {
  lazy val success = entry.term != NO_TERM
}
