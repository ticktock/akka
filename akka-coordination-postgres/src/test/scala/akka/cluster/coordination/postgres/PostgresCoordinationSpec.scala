package akka.cluster.coordination.postgres
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.cluster.coordination._
import java.util.concurrent.{ TimeUnit, CountDownLatch }
import akka.cluster.storage.{ BadVersionException, DataExistsException }

class PostgresCoordinationSpec extends WordSpec with MustMatchers {

  val client = new PostgresCoordinationClient

  "A coordination client" must {
    "create data successfully" in {
      val path = stampedPath("/path/to/create/test")
      client.create(path, path)
      client.read[String](path) must be(path)
    }

    "throw a data exists exception when trying to create an existing path" in {
      val path = stampedPath("/path/to/dup/create/test")
      client.create(path, path)
      evaluating(client.create(path, path)) must produce[DataExistsException]
    }

    "must check existence properly" in {
      val path = stampedPath("/path/to/exists")
      client.exists(path) must be(false)
      client.createPath(path)
      client.exists(path) must be(true)
    }

    "must update data successfully" in {
      val path = stampedPath("/path/to/update")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      client.update(path, updated, version)
      client.read[String](path) must be(updated)
    }

    "must throw a BadVersionException" in {
      val path = stampedPath("/path/to/update/badversion")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      evaluating(client.update(path, updated, version - 1)) must produce[BadVersionException]
    }

    "successfully listen" in {
      val latch = new CountDownLatch(2)
      val l = new CoordinationNodeListener {
        def handleChange(path: String, children: List[String]) = {
          latch.countDown()
        }
      }
      val path = stampedPath("/path/to/foo")
      client.createPath(path)
      client.listenTo(path, l)
      client.create(path + "/bar", "baz")
      client.delete(path + "/bar")
      latch.await(2, TimeUnit.SECONDS) must be(true)

    }
  }

  def stampedPath(path: String): String = {
    path + (System.currentTimeMillis().toString)
  }
}