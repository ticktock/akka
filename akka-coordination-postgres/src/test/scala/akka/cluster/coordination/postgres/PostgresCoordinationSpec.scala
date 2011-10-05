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

    /*"must throw a BadVersionException" in {
      val path = stampedPath("/path/to/update/badversion")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      evaluating(client.update(path, updated, version - 1)) must produce[BadVersionException]
    }  */

    "must force update data successfully" in {
      val path = stampedPath("/path/to/force/update")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      client.forceUpdate(path, updated)
      client.read[String](path) must be(updated)
      client.readData(path).version must be(version + 1)
    }

    "must delete data successfully" in {
      val path = stampedPath("/path/to/force/update")
      client.create(path, path)
      client.exists(path) must be(true)
      client.delete(path)
      client.exists(path) must be(false)
    }

    "successfully listen" in {
      val latch = new CountDownLatch(2)
      val l = new CoordinationNodeListener {
        def handleChange(path: String, children: List[String]) = {
          latch.countDown()
        }
      }
      val path = stampedPath("/path/to/listen")
      client.createPath(path)
      client.listenTo(path, l)
      client.create(path + "/bar", "baz")
      client.delete(path + "/bar")
      latch.await(2, TimeUnit.SECONDS) must be(true)

    }

    "succesfully unlisten" in {
      val latch = new CountDownLatch(1)
      val l = new CoordinationNodeListener {
        def handleChange(path: String, children: List[String]) = {
          latch.countDown()
        }
      }
      val path = stampedPath("/path/to/unlisten")
      client.createPath(path)
      client.listenTo(path, l)
      client.stopListenTo(path, l)
      client.create(path + "/bar", "baz")
      client.delete(path + "/bar")
      latch.await(1, TimeUnit.SECONDS) must be(false)
    }

    "succesfully unlisten all" in {
      val latch = new CountDownLatch(1)
      val l = new CoordinationNodeListener {
        def handleChange(path: String, children: List[String]) = {
          latch.countDown()
        }
      }
      val path = stampedPath("/path/to/unlistenall1")
      val path2 = stampedPath("/path/to/unlistenall2")
      client.createPath(path)
      client.createPath(path2)
      client.listenTo(path, l)
      client.stopListenAll()
      client.create(path + "/bar", "baz")
      client.delete(path2 + "/bar")
      latch.await(1, TimeUnit.SECONDS) must be(false)
    }

    "successfully get children" in {
      val path = stampedPath("/path/to/get/children")
      val child1 = path + "/child1"
      val child2 = path + "/child2"
      client.createPath(path)
      client.create(child1, child1)
      client.create(child2, child2)
      client.getChildren(path) must be(List(child1, child2))
    }
  }

  def stampedPath(path: String): String = {
    path + (System.currentTimeMillis().toString)
  }
}