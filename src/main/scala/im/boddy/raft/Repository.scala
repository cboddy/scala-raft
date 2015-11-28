package im.boddy.raft

import scala.collection.mutable.ArrayBuffer

case class Entry(index: Index, term: Term)
case class LogEntry[T](id: Entry, value : T)

trait LogRepository[T] {

  def getEntries(start: Index, end:  Index) : Seq[LogEntry[T]]
  def getEntry(entryKey: Index) : LogEntry[T] = getEntries(entryKey, entryKey+1)(0)

  def containsEntry(entryKey: Entry) : Boolean

  def putEntries(entries: Seq[LogEntry[T]]) : Unit
  def putEntry(entry: LogEntry[T]) = putEntries(Seq(entry))

}

class BufferLogRepository[T] extends LogRepository[T] {

  private[raft] var log = new ArrayBuffer[LogEntry[T]]()

  override def getEntries(start: Index, end:Index): Seq[LogEntry[T]] = {
    if (log.size < end) throw new IllegalStateException()
    log.slice(start.toInt, end.toInt)
  }

  override def putEntries(entries: Seq[LogEntry[T]]) = {
    val index = entries.head.id.index.toInt
    val delta = index - log.size
    delta match {
      case _ if delta < 0 => log = log.slice(0, index)
    }
  }

  override def containsEntry(key: Entry): Boolean = {
    val pos = key.index.toInt
    if (log.size <= pos) false
    log(pos).id.term == key.term
  }
}