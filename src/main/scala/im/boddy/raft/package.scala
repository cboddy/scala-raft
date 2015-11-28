package im.boddy

import java.util.concurrent.TimeUnit
import java.util.logging.Logger

package object raft {

  type Term = Long
  type Id = Long
  type Index = Long
  type ClientId = Long
  type RequestId = Long

  val NOT_VOTED : Id = -1
  val NO_LEADER : Id = -1
  val NO_TERM : Term =  -1
  val NO_PING_SENT: Long = 0

  def now = System.currentTimeMillis()

  case class Duration(count: Int, unit: TimeUnit) extends Ordered[Duration] {
    lazy val toMillis : Long = unit.toMillis(count)
    override def compare(that: Duration) = this.toMillis.compareTo(that.toMillis)
  }

  case class Config(peers: Seq[Id]) {
    lazy val size = peers.size
    lazy val majority = size/2+1
  }
}
