package im.boddy.raft

import scala.collection.mutable.ArrayBuffer

case class LogEntry[T](index: Index, value : T)

trait LogRepository[T] {

  def getEntries(startIndex: Index, endIndex: Index) : Seq[LogEntry[T]]
  def getEntry(index: Index) : LogEntry[T] = getEntries(index, index+1)(0)

  def containsEntry(index: Index) : Boolean

  def putEntries(entries: Seq[LogEntry[T]]) : Unit
  def putEntry(entry: LogEntry[T]) = putEntries(Seq(entry))

}

class BufferLogRepository[T] extends LogRepository[T] {

  val log = new ArrayBuffer[LogEntry[T]]()

  override def getEntries(startIndex: Index, endIndex: Index): Seq[LogEntry[T]] = {
    val length: Int = log.size
    if (length < endIndex) throw new IllegalStateException("Requested index "+ endIndex +" past limit "+ length)
    log.slice(startIndex.toInt, endIndex.toInt)
  }

  override def putEntries(entries: Seq[LogEntry[T]]) = log ++= entries

  override def containsEntry(index: Index) : Boolean = try {
    getEntry(index)
    true
  } catch {
    case _ => false
  }
}