package akka.cluster.coordination.postgres
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.cluster.coordination._
import java.util.concurrent.{ TimeUnit, CountDownLatch }
import PostgresCoordinationClient._
import resource._
import akka.cluster.storage.{ MissingDataException, BadVersionException, DataExistsException }

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

    "check existence properly" in {
      val path = stampedPath("/path/to/exists")
      client.exists(path) must be(false)
      client.createPath(path)
      client.exists(path) must be(true)
    }

    "update data successfully" in {
      val path = stampedPath("/path/to/update")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      client.update(path, updated, version)
      client.read[String](path) must be(updated)
    }

    "throw a BadVersionException when updating with the wrong version" in {
      val path = stampedPath("/path/to/update/badversion")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      evaluating(client.update(path, updated, version - 1)) must produce[BadVersionException]
    }

    "throw a MissingDataException when updating non existent path" in {
      val path = stampedPath("/path/to/update/missingdata")
      evaluating {
        client.update(path, path, 2)
      } must produce[MissingDataException]
      evaluating(client.forceUpdate(path, path)) must produce[MissingDataException]
    }

    "throw a MissingDataException when reading non existent path" in {
      val path = stampedPath("/path/to/read/missingdata")
      evaluating {
        client.readData(path)
      } must produce[MissingDataException]

    }

    "force update data successfully" in {
      val path = stampedPath("/path/to/force/update")
      client.create(path, path)
      val (rpath, version) = client.readWithVersion[String](path)
      val updated = stampedPath(path)
      client.forceUpdate(path, updated)
      client.read[String](path) must be(updated)
      client.readData(path).version must be(version + 1)
    }

    "delete data successfully" in {
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

    "successfully timeout ephemerals and delete ephemerals on close" in {
      val path = stampedPath("/path/to/ephemeral/timeout")

      managed(new PostgresCoordinationClient).acquireAndGet { client2 ⇒
        {
          (client2.coordActor ? Insert(path, null, EPHEMERAL, EphemeralTimeout("1 sec"))).as[Return[String]].get.fold(es ⇒ throw es.head, s ⇒ s)
          client.exists(path) must be(true);
          Thread.sleep(1000)
          client.createPath(path + "123")
          //should timeout the ephemeral
          client.exists(path) must be(false)
          client2.createEphemeralPath(path)
          client.exists(path) must be(true)
        }
      }
      client.exists(path) must be(false)
    }

    "create ephemeral sequentials correctly" in {
      val path = stampedPath("/path/to/ephemeral/sequential")
      val one = client.createEphemeralSequential(path, "foo")
      one.endsWith("_0000000001") must be(true)
      val two = client.createEphemeralSequential(path, "foo")
      two.endsWith("_0000000002") must be(true)
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

    "recursively delete nodes" in {
      val path = stampedPath("/path/to/recursive/delete")
      client.createPath(path)
      val child = path + "/child"
      client.createPath(child)
      client.exists(child) must be(true)
      client.deleteRecursive(path)
      client.exists(child) must be(false)
    }
  }

  def stampedPath(path: String): String = {
    path + (System.currentTimeMillis().toString)
  }
}