package im.boddy.raft.memory

import java.util.concurrent.TimeUnit
import java.util.logging.Level

import im.boddy.raft._
import org.specs2.mutable._
import org.specs2.specification.AfterAll

class MemorySpec extends Specification with AfterAll with Logging {

  log.setLevel(Level.FINEST)

  val ids = (0 until 3).map(_.toLong)
  val timeout: Duration = Duration(1, TimeUnit.SECONDS)
  val broker = new AsyncBroker[Int](Config(ids), timeout)
  lazy val peers = for (id <- ids) yield broker.addPeer(id, timeout)
  "Memory Raft System" should {

    "start the threadpool" in {
      peers
      Thread.sleep(1000*10)
      ok
    }

  }

  override def afterAll {
    peers.foreach(_.close)
    broker.shutdown
  }

}
