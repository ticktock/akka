package akka.cluster.coordination.postgres

import akka.cluster.coordination.{ CoordinationNodeListener, CoordinationConnectionListener, CoordinationLockListener, CoordinationClient }
import org.postgresql.PGConnection
import PostgresCoordinationClient._
import javax.sql._
import akka.actor._
import akka.actor.Actor._
import akka.event.EventHandler._
import akka.cluster.ChangeListener._
import akka.serialization.JavaSerializer
import akka.util.Helpers._
import org.postgresql.util._
import resource._
import java.sql._
import java.util.Arrays
import collection.JavaConversions._
import scala.Array
import java.net.URI
import scala.util.Properties
import akka.cluster.storage.{ MissingDataException, BadVersionException, VersionedData }
import org.postgresql.ds._
import collection.mutable.{ ArrayBuffer, HashMap, HashSet, LinkedHashSet }
import java.util.concurrent.TimeUnit

object PostgresCoordinationClient {

  class NodeType extends PGobject {
    setType("node_type")
  }

  type PGConn = Connection with PGConnection

  case object PERSISTENT extends NodeType {
    setValue("PERSISTENT")
  }

  case object EPHEMERAL extends NodeType {
    setValue("EPHEMERAL")
  }

  case object EPHEMERAL_SEQUENTIAL extends NodeType {
    setValue("EPHEMERAL_SEQUENTIAL")
  }

  case class EphemeralTimeout(timeout: String = "1 min") {
    def interval: PGInterval = {
      val i = new PGInterval()
      i.setValue(timeout)
      i
    }
  }

  type Return[T] = Either[List[Throwable], T]

  sealed trait CoordinationOp
  case class Insert(path: String, value: Array[Byte], nodeType: NodeType = PERSISTENT, timeout: EphemeralTimeout = EphemeralTimeout()) extends CoordinationOp
  case class Read(path: String, version: Option[Long] = None) extends CoordinationOp
  case class Update(path: String, value: Array[Byte], version: Option[Long] = None) extends CoordinationOp
  case class Delete(path: String) extends CoordinationOp
  case class DeleteRecursive(path: String) extends CoordinationOp {
    def recursive = if (path.endsWith("/")) (path + "%") else (path + "/%")
  }
  case class Exists(path: String) extends CoordinationOp
  case class Listen(path: String, listener: CoordinationNodeListener) extends CoordinationOp
  case class ListenConnection(listener: CoordinationConnectionListener) extends CoordinationOp
  case class Unlisten(path: String, listener: CoordinationNodeListener) extends CoordinationOp
  case class UnlistenConnection(listener: CoordinationConnectionListener) extends CoordinationOp
  case class Notify(path: String, children: List[String]) //not a coordination op
  case class GetChildren(path: String) extends CoordinationOp
  case object UnlistenAll extends CoordinationOp
  case object TouchEphemeralsAndDoNotification
}

class PostgresClient(conn: PGConn, coordinationActor: ActorRef) {

  val uuid = coordinationActor.uuid
  val serializer = new JavaSerializer

  def close() = conn.close()

  def withPreparedStatement[T](statement: String)(block: PreparedStatement ⇒ T): Return[T] = {
    managed(conn.prepareStatement(statement)).acquireFor(block)
  }

  def withCallableStatement[T](statement: String)(block: CallableStatement ⇒ T): Return[T] = {
    managed(conn.prepareCall(statement)).acquireFor(block)
  }

  def expect(num: Int, block: ⇒ Int) = {
    val res = block
    if (res != num) throw new SQLException("Expected %d updates but got %d".format(num, res))
  }

  def expectMore(num: Int, block: ⇒ Int) = {
    val res = block
    if (res <= num) throw new SQLException("Expected %d updates but got %d".format(num, res))
  }

  def insert(in: Insert): Return[String] = {
    val fn = { stmt: CallableStatement ⇒
      import stmt._
      setString(1, in.path)
      setBytes(2, in.value)
      setString(3, uuid.toString)
      setObject(4, in.timeout.interval)
      executeUpdate()
      managed(getResultSet).acquireAndGet { rs ⇒
        rs.next()
        rs.getString(1)
      }
    }
    in.nodeType match {
      case PERSISTENT           ⇒ withCallableStatement("{call create_persistent(?,?,?,?) }")(fn)
      case EPHEMERAL            ⇒ withCallableStatement("{call create_ephemeral(?,?,?,?) }")(fn)
      case EPHEMERAL_SEQUENTIAL ⇒ withCallableStatement("{call create_ephemeral_sequential(?,?,?,?) }")(fn)
    }
  }

  def exists(e: Exists): Return[Boolean] = withPreparedStatement("SELECT PATH FROM AKKA_COORDINATION WHERE PATH = ?") { stmt ⇒
    {
      stmt.setString(1, e.path)
      managed(stmt.executeQuery()).acquireAndGet { rs ⇒
        if (rs.next() && (!rs.next())) true else false
      }
    }
  }

  def read(r: Read): Return[VersionedData] = withPreparedStatement {
    """
   SELECT VALUE, VERSION FROM AKKA_COORDINATION WHERE PATH = ?
   """
  } { stmt ⇒
    import stmt._
    setString(1, r.path)
    managed(executeQuery()).acquireAndGet { rs ⇒
      rs.next()
      new VersionedData(rs.getBytes(1), rs.getLong(2))
    }
  }

  def getChildren(g: GetChildren): Return[List[String]] = withPreparedStatement("SELECT '/' || PATH FROM AKKA_COORDINATION_PATHS WHERE PARENT = ? ORDER BY PATH") { stmt ⇒
    import stmt._
    setString(1, g.path.tail)
    managed(executeQuery()).acquireAndGet { rs ⇒
      val list = new ArrayBuffer[String]()
      while (rs.next()) {
        list add rs.getString(1)
      }
      list.toList
    }
  }

  def update(u: Update): Return[VersionedData] = withCallableStatement("{call update_node(?,?,?)}") { stmt ⇒
    import stmt._
    setString(1, u.path)
    setBytes(2, u.value)
    u.version match {
      case Some(version) ⇒ setLong(3, version)
      case None          ⇒ setNull(3, Types.BIGINT)
    }
    executeUpdate()
    managed(getResultSet).acquireAndGet { rs ⇒
      rs.next()
      new VersionedData(u.value, rs.getLong(1))
    }
  }

  def delete(d: Delete): Return[Boolean] = withPreparedStatement("DELETE FROM AKKA_COORDINATION WHERE PATH = ?") { stmt ⇒
    import stmt._
    setString(1, d.path)
    executeUpdate() == 1
  }

  def deleteRecursive(d: DeleteRecursive): Return[Boolean] = withPreparedStatement("DELETE FROM AKKA_COORDINATION WHERE PATH = ? OR PATH LIKE ?") { stmt ⇒
    import stmt._
    setString(1, d.path)
    setString(2, d.recursive)
    executeUpdate() > 0
  }

  def deleteEphemerals(): Return[Unit] = withPreparedStatement("DELETE FROM AKKA_COORDINATION WHERE CREATOR = ? AND NODE IN ('EPHEMERAL', 'EPHEMERAL_SEQUENTIAL') ") { stmt ⇒
    import stmt._
    setString(1, uuid.toString)
    executeUpdate()
    ()
  }

  def listen(l: Listen): Return[Unit] =
    managed(conn.createStatement()).acquireFor { stmt ⇒
      import stmt._
      expect(0, executeUpdate("""LISTEN "%s" """.format(l.path)))
    }

  def unlisten(u: Unlisten): Return[Unit] = managed(conn.createStatement()).acquireFor { stmt ⇒
    import stmt._
    expect(0, executeUpdate("""UNLISTEN "%s" """.format(u.path)))
  }

  def touchEphemeralsAndDoNotification(): Return[Unit] = withPreparedStatement("UPDATE AKKA_COORDINATION SET UPDATED = CURRENT_TIMESTAMP WHERE CREATOR = ?") { stmt ⇒
    stmt.setString(1, uuid.toString)
    stmt.executeUpdate()

    for {
      ns ← Option(conn.getNotifications)
      n ← ns
    } coordinationActor ! Notify(n.getName, parseNotification(n.getParameter))

  }

  private def parseNotification(param: String): List[String] = {
    if (param.contains("|")) param.split("|").toList else List(param)
  }

}

class CoordinationActor extends Actor {

  val pgClient: PostgresClient = new PostgresClient(DS.conn(), self)
  val nodeListeners = new HashMap[String, LinkedHashSet[CoordinationNodeListener]]
  val connectionListeners = new HashSet[CoordinationConnectionListener]
  var keepalive = Scheduler.schedule(self, TouchEphemeralsAndDoNotification, 2L, 2L, TimeUnit.SECONDS)

  override def postStop() = {
    pgClient.deleteEphemerals()
    pgClient.close
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    keepalive.cancel(true)
    asScalaSet(connectionListeners).foreach(l ⇒ spawn(l.handleEvent(NodeDisconnected("coordination-client"))))
  }

  override def postRestart(reason: Throwable) = {
    asScalaSet(connectionListeners).foreach(l ⇒ spawn(l.handleEvent(NodeConnected("coordination-client"))))
    keepalive = Scheduler.schedule(self, TouchEphemeralsAndDoNotification, 2L, 2L, TimeUnit.SECONDS)
  }

  protected def receive = {

    case Notify(path, children) ⇒ for {
      listeners ← nodeListeners.get(path)
      l ← listeners
    } spawn(l.handleChange(path, children))
    case TouchEphemeralsAndDoNotification ⇒ { pgClient.touchEphemeralsAndDoNotification() }

    case op: CoordinationOp ⇒ {
      op match {
        case i: Insert          ⇒ channel ! pgClient.insert(i)
        case r: Read            ⇒ channel ! pgClient.read(r)
        case u: Update          ⇒ channel ! pgClient.update(u)
        case d: Delete          ⇒ channel ! pgClient.delete(d)
        case d: DeleteRecursive ⇒ channel ! pgClient.deleteRecursive(d)
        case e: Exists          ⇒ channel ! pgClient.exists(e)
        case g: GetChildren     ⇒ channel ! pgClient.getChildren(g)

        case l @ Listen(path, listener) ⇒ {
          nodeListeners.getOrElseUpdate(path, new LinkedHashSet[CoordinationNodeListener]).add(listener)
          channel ! pgClient.listen(l)
        }

        case u @ Unlisten(path, listener) ⇒ {
          nodeListeners.get(path).foreach(_.remove(listener))
          channel ! pgClient.unlisten(u)
        }

        case UnlistenAll ⇒ channel ! {
          nodeListeners.clear
          pgClient.unlisten(Unlisten("*", null))
        }

        case UnlistenConnection(listener) ⇒ channel ! { connectionListeners.remove(listener); () }
        case ListenConnection(listener)   ⇒ channel ! { connectionListeners.add(listener); () }
      }
    }

  }
}

class PostgresCoordinationClient extends CoordinationClient {

  val coordActor = actorOf[CoordinationActor]
  val serializer = new JavaSerializer

  private def handleOp[T](op: CoordinationOp, ex: ToStorageException)(implicit manifest: Manifest[Return[T]]): T = {
    (coordActor ? op).as[Return[T]] match {
      case Some(ret) ⇒ {
        ret.fold(fail ⇒ handleWith(ex) { throw fail.head }, succ ⇒ succ)
      }
      case None ⇒ {
        //cant rety because of mailbox ordering
        handleWith(ex) { throw new SQLTimeoutException("request timed out") }
      }
    }
  }

  def close() = coordActor.stop()

  def createData(path: String, value: Array[Byte]) = handleOp[Unit](Insert(path, value), createFailed(path))

  def create(path: String, value: AnyRef) = createData(path, serializer.toBinary(value))

  def createEphemeralSequentialData(path: String, value: Array[Byte]) = handleOp[String](Insert(path, value, EPHEMERAL_SEQUENTIAL), createFailed(path))

  def createEphemeralData(path: String, value: Array[Byte]) = handleOp[Unit](Insert(path, value, EPHEMERAL), createFailed(path))

  def createEphemeralPath(path: String) = createEphemeralData(path, null)

  def createEphemeralSequentialPath(path: String) = createEphemeralSequentialData(path, null)

  def createPath(path: String) = createData(path, null)

  def createEphemeral(path: String, value: AnyRef) = createEphemeralData(path, serializer.toBinary(value))

  def createEphemeralSequential(path: String, value: AnyRef) = createEphemeralSequentialData(path, serializer.toBinary(value))

  def read[T](path: String) = serializer.fromBinary(readData(path).data).asInstanceOf[T]

  def readWithVersion[T](path: String) = {
    val versioned = handleOp[VersionedData](Read(path), readDataFailed(path))
    (serializer.fromBinary(versioned.data).asInstanceOf[T], versioned.version)
  }

  def readData(path: String) = handleOp[VersionedData](Read(path), readDataFailed(path))

  def readData(path: String, version: Long) = handleOp[VersionedData](Read(path), readDataFailed(path))

  def forceUpdateData(path: String, value: Array[Byte]) = handleOp[VersionedData](Update(path, value), writeDataFailed(path))

  def forceUpdate(path: String, value: AnyRef) = forceUpdateData(path, serializer.toBinary(value))

  def update(path: String, value: AnyRef, version: Long) = updateData(path, serializer.toBinary(value), version)

  def updateData(path: String, value: Array[Byte], expectedVersion: Long) = handleOp[VersionedData](Update(path, value, Some(expectedVersion)), writeDataFailed(path))

  def delete(path: String) = handleOp[Boolean](Delete(path), deleteFailed(path))

  def deleteRecursive(path: String) = handleOp[Boolean](DeleteRecursive(path), deleteRecursiveFailed(path))

  def exists(path: String) = handleOp[Boolean](Exists(path), existsFailed(path))

  def getChildren(path: String) = handleOp[List[String]](GetChildren(path), defaultStorageException)

  def getLock(path: String, listener: CoordinationLockListener) = null

  def listenTo(path: String, listener: CoordinationNodeListener) = handleOp[Unit](Listen(path, listener), defaultStorageException)

  def listenToConnection(listener: CoordinationConnectionListener) = handleOp[Unit](ListenConnection(listener), defaultStorageException)

  def reconnect() = null

  def retryUntilConnected[T](code: ⇒ T) = code

  def serverAddresses = null

  def stopListenAll() = handleOp[Unit](UnlistenAll, defaultStorageException)

  def stopListenTo(path: String, listener: CoordinationNodeListener) = handleOp[Unit](Unlisten(path, listener), defaultStorageException)

  def stopListenToConnection(listener: CoordinationConnectionListener) = handleOp[Unit](UnlistenConnection(listener), defaultStorageException)

  private def deleteFailed(key: String): ToStorageException = {
    case e: Exception ⇒ CoordinationClient.deleteFailed(key, e)
  }

  private def deleteRecursiveFailed(key: String): ToStorageException = {
    case e: Exception ⇒ CoordinationClient.deleteRecursiveFailed(key, e)
  }

  private def writeDataFailed(key: String): ToStorageException = {
    case e: SQLException if false ⇒ CoordinationClient.writeDataFailedBadVersion(key, e)
    case e: SQLException if false ⇒ CoordinationClient.writeDataFailedBadVersion(key, e)
    case e: SQLException if false ⇒ CoordinationClient.writeDataFailedMissingData(key, e)

  }

  private def readDataFailed(key: String): ToStorageException = {
    case e: SQLException if false ⇒ CoordinationClient.readDataFailedMissingData(key, e)
    case e: SQLException if false ⇒ CoordinationClient.readDataFailedMissingData(key, e)
    case e: SQLException          ⇒ CoordinationClient.readDataFailed(key, e)
  }

  private def existsFailed(key: String): ToStorageException = {
    case e: SQLException if false ⇒ CoordinationClient.existsFailed(key, e)
  }

  private def createFailed(key: String): ToStorageException = {
    case e: PSQLException if e.getSQLState.equals("23505") ⇒ CoordinationClient.createFailedDataExists(key, e)
    case e: SQLException ⇒ {
      error(e, this, "State:" + e.getSQLState)
      CoordinationClient.createFailed(key, e)
    }
  }
}

/*
 s.executeUpdate(""" LISTEN "/path/to/foo" """)

*/

object DS {

  val url = new URI(Properties.envOrElse("DATABASE_URL", "postgres://user:pass@host/database"))

  //get arm working with autocommit false commits/
  val ds = new PGSimpleDataSource
  ds.setServerName(url.getHost)
  val userInfo = url.getUserInfo.split(":")
  ds.setPassword(userInfo(1))
  ds.setUser(userInfo(0))
  ds.setDatabaseName(url.getPath.tail)
  ds.setSsl(true)
  ds.setSslfactory("org.postgresql.ssl.NonValidatingFactory")

  def conn(): PGConn = {
    ds.getConnection.asInstanceOf[PGConn]
  }
}

class ConnListener extends ConnectionEventListener {
  def connectionClosed(event: ConnectionEvent) {
    println(event.getSQLException.getStackTraceString)
  }

  def connectionErrorOccurred(event: ConnectionEvent) {
    println(event.getSQLException.getStackTraceString)
  }
}