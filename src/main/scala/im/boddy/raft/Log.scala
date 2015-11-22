package im.boddy.raft

case class LogEntry[T](index: Index, value : T)

abstract class LogRepository[T] {

  def getEntries(startIndex: Index, endIndex: Index) : Seq[LogEntry[T]]
  def getEntry(index: Index) : LogEntry[T] = getEntries(index, index+1)(0)

  def putEntries(entries: Seq[LogEntry[T]]) : Unit
  def putEntry(entry: LogEntry[T]) = putEntries(Seq(entry))

}