package im.boddy.raft

abstract class PDU(term: Term)

case class SourcedPDU(source: Id, pdu: PDU)

case class AppendEntries[T](term: Term,
                          leaderId: Id,
                          previousIndex: Index,
                          previousTerm: Term,
                          entries : Seq[LogEntry[T]],
                          leaderCommit: Index
                          ) extends PDU(term) {
  lazy val committedIndex = Math.min(leaderCommit, entries.last.index)
}

case class RequestVote(term: Term,
                       candidate: Id,
                       lastLogIndex: Index,
                       lastLogTerm: Index) extends PDU(term)


object AppendState extends Enumeration {
  val TERM_NOT_CURRENT, MISSING_PREVIOUS_ENTRY, SUCCESS = Value
}

case class AppendEntriesAck(term: Term, state: AppendState.Value) extends PDU(term) {
  def success = state == AppendState.SUCCESS
}


case object RequestVoteState extends Enumeration {
  val TERM_NOT_CURRENT, CANDIDATE_MISSING_PREVIOUS_ENTRY, SUCCESS = Value
}

case class RequestVoteAck(term: Term, state: RequestVoteState.Value) extends PDU(term) {
  def success = state == RequestVoteState.SUCCESS
}