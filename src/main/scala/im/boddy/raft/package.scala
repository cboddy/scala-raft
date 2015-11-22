package im.boddy

import java.util.logging.Logger

package object raft {
  type Term = Long
  type Id = Long
  type Index = Long
  type Timeout = Long

  val NOT_VOTED : Id = -1
  val NO_LEADER : Id = -1

  trait Logging {
    def log = Logger.getGlobal
  }

}
