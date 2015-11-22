package im.boddy

import java.util.logging.Logger

package object raft {
  type Term = Long
  type Id = Long
  type Index = Long
  type Timeout = Long

  trait Logging {
    def log = Logger.getGlobal
  }

}
