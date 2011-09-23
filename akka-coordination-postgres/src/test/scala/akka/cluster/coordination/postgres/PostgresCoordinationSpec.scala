package akka.cluster.coordination.postgres
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.cluster.coordination._

class PostgresCoordinationSpec extends WordSpec with MustMatchers {

  "A coordination client" must {
    "successfully listen" in {
      val l = new CoordinationNodeListener with Comparable[CoordinationNodeListener] {
        def handleChange(path: String, children: List[String]) = {
          println("changed %s, %s".format(path, children.toString()))
        }

        def compareTo(o: CoordinationNodeListener) = this.hashCode() - o.hashCode()
      }
      val stamp = System.currentTimeMillis().toString
      val client = new PostgresCoordinationClient
      val path = "/path/to/foo" + stamp
      client.createPath(path)
      client.listenTo(path, l)
      client.create(path + "/bar", "baz")
      client.delete(path + "/bar")
      client.exists(path)
      println("exists")
      client.close

    }
  }

}