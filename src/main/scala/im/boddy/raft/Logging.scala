package im.boddy.raft

import java.util.logging.Logger


trait Logging {
  def log = Logger.getGlobal
}
