package im.boddy.raft

import scala.collection.mutable.ArrayBuffer

case class EntryKey(index: Index, term: Term)
case class LogEntry[T](id: EntryKey, value : T)

trait LogRepository[T] {

  def getEntries(entries: Seq[(EntryKey)]) : Seq[LogEntry[T]]
  def getEntry(entryKey: EntryKey) : LogEntry[T] = getEntries(Seq((entryKey)))(0)

  def containsEntry(entryKey: EntryKey) : Boolean

  def putEntries(entries: Seq[LogEntry[T]]) : Unit
  def putEntry(entry: LogEntry[T]) = putEntries(Seq(entry))

}

class BufferLogRepository[T] extends LogRepository[T] {

  private[raft] var log = new ArrayBuffer[Option[LogEntry[T]]]()

  override def getEntries(entries: Seq[(EntryKey)]): Seq[LogEntry[T]] = {
    val last = entries.last
    if (!containsEntry(entries.last))
      throw new IllegalStateException()
    entries.map(e => log(e.index.toInt).get)
  }

  override def putEntries(entries: Seq[LogEntry[T]]) = entries.foreach(e => {

    entries.sortBy(_.id.index).foreach(e => {

      val index : Int = e.id.index.toInt
      val delta = log.size - index

      delta match {
        case x if x < 0 => log = log.slice(0, index)
        case x if x > 0 => log ++ (0 to delta).map(e => None)
      }
      log += Some(e)
    })
  })

  override def containsEntry(key: EntryKey): Boolean = {
    val pos = key.index.toInt
    if (log.size <= pos) false
    val opt = log(pos)
    opt.nonEmpty && opt.get.id.term == key.term
  }
}