package im.boddy

import java.util.concurrent.TimeUnit
import java.util.logging.Logger

package object raft {

  type Term = Long
  type Id = Long
  type Index = Long

  val NOT_VOTED : Id = -1
  val NO_LEADER : Id = -1
  val NO_TERM : Term =  -1

  trait Logging {
    def log = Logger.getGlobal
  }

  case class Timeout(count: Int, unit: TimeUnit)
}
