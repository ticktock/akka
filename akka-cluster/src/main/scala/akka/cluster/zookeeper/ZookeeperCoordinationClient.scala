package akka.cluster.zookeeper

import java.util.{ List ⇒ JList }
import collection.JavaConversions._
import org.I0Itec.zkclient.{ IZkStateListener, IZkChildListener }
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.Watcher.Event.KeeperState
import akka.cluster.ChangeListener._
import akka.cluster.storage.VersionedData
import akka.cluster.coordination._
import org.apache.zookeeper.recipes.lock.WriteLock
import java.util.concurrent.{ Callable, ConcurrentHashMap }
import akka.util.Duration
import akka.config.Config._
import org.apache.zookeeper.{ CreateMode, KeeperException }
import org.I0Itec.zkclient.exception.{ ZkNoNodeException, ZkBadVersionException, ZkNodeExistsException }

class ZookeeperCoordinationClient(zkClient: AkkaZkClient) extends CoordinationClient {

  val nodeListeners = new ConcurrentHashMap[CoordinationNodeListener, IZkChildListener]()
  val connectionListeners = new ConcurrentHashMap[CoordinationConnectionListener, IZkStateListener]()

  def close() = zkClient.close()

  def serverAddresses: String = zkClient.serverAddresses

  def stopListenToConnection(listener: CoordinationConnectionListener) {
    handle {
      val zkListener = connectionListeners(listener)
      zkClient.unsubscribeStateChanges(zkListener)
      connectionListeners.remove(listener)
    }
  }

  def listenToConnection(listener: CoordinationConnectionListener) {
    handle {
      val zkListener = new ZookeeperCoordinationConnectionListener(listener)
      zkClient.subscribeStateChanges(zkListener)
      connectionListeners += (listener -> zkListener)
      ()
    }
  }

  def stopListenTo(path: String, listener: CoordinationNodeListener) {
    handle {
      val zkListener = nodeListeners(listener)
      zkClient.unsubscribeChildChanges(path, zkListener)
      nodeListeners.remove(listener)
      ()
    }
  }

  def listenTo(path: String, listener: CoordinationNodeListener) {
    handle {
      val zkListener = new ZookeeperCoordinationNodeListener(listener)
      zkClient.subscribeChildChanges(path, zkListener)
      nodeListeners += (listener -> zkListener)
      ()
    }
  }

  def deleteRecursive(path: String): Boolean = handleWith(deleteRecursiveFailed(path)) {
    zkClient.deleteRecursive(path)
  }

  def delete(path: String): Boolean = handleWith(deleteFailed(path)) {
    zkClient.delete(path)
  }

  def getChildren(path: String): List[String] = handle {
    List(asScalaBuffer(zkClient.getChildren(path)).toArray: _*)
  }

  def forceUpdateData(path: String, value: Array[Byte]): VersionedData = handleWith(writeDataFailed(path)) {
    val stat = new Stat()
    zkClient.connection.writeData(path, value, -1)
    new VersionedData(value, stat.getVersion.toLong)
  }

  def updateData(path: String, value: Array[Byte], expectedVersion: Long): VersionedData = handleWith(writeDataFailed(path)) {
    val stat = new Stat()
    zkClient.connection.writeData(path, value, expectedVersion.toInt)
    new VersionedData(value, stat.getVersion.toLong)
  }

  def readData(path: String): VersionedData = handleWith(readDataFailed(path)) {
    val stat = new Stat()
    val data = zkClient.connection.readData(path, stat, false)
    new VersionedData(data, stat.getVersion.toLong)
  }

  def readData(path: String, version: Long): VersionedData = {
    val verData = readData(path)
    if (verData.version != version) {
      throw CoordinationClient.readDataFailedBadVersion(path, version, verData.version)
    }
    verData
  }

  def exists(path: String): Boolean = handleWith(existsFailed(path)) {
    zkClient.exists(path)
  }

  def retryUntilConnected[T](block: ⇒ T): T = {
    zkClient.retryUntilConnected(new Callable[T] {
      def call(): T = block
    })
  }

  def getLock(path: String, listener: CoordinationLockListener): CoordinationLock = {
    val lock = new WriteLock(zkClient.connection.getZookeeper, path, null, new ZookeeperLockListener(listener))
    new ZookeeperCoordinationLock(lock)
  }

  /*Exception handling partial functions that map store specific exceptions to generic exceptions*/

  def readWithVersion[T](path: String): (T, Long) = {
    val verData = readData(path)
    (zkClient.zkSerializer.deserialize(verData.data).asInstanceOf[T], verData.version)
  }

  def update(path: String, value: AnyRef, version: Long) = updateData(path, zkClient.zkSerializer.serialize(value), version)

  def createEphemeral(path: String, value: AnyRef) = handleWith(createFailed(path)) {
    zkClient.createEphemeral(path, value)
  }

  def createEphemeralSequential(path: String, value: AnyRef): String = handleWith(createFailed(path)) {
    zkClient.createEphemeralSequential(path, value)
  }

  def createData(path: String, value: Array[Byte]) = handleWith(createFailed(path)) {
    zkClient.connection.create(path, value, CreateMode.PERSISTENT)
  }

  def createEphemeralData(path: String, value: Array[Byte]) = handleWith(createFailed(path)) {
    zkClient.connection.create(path, value, CreateMode.EPHEMERAL)
  }

  def createEphemeralSequentialData(path: String, value: Array[Byte]) = handleWith(createFailed(path)) {
    zkClient.connection.create(path, value, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  def createPath(path: String) = handleWith(createFailed(path)) {
    zkClient.connection.create(path, null, CreateMode.PERSISTENT)
  }

  def createEphemeralPath(path: String) = handleWith(createFailed(path)) {
    zkClient.connection.create(path, null, CreateMode.EPHEMERAL)
  }

  def createEphemeralSequentialPath(path: String) = handleWith(createFailed(path)) {
    zkClient.connection.create(path, null, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  def create(path: String, value: AnyRef) = handleWith(createFailed(path)) {
    zkClient.createPersistent(path, value)
  }

  def read[T](path: String): T = zkClient.zkSerializer.deserialize(readData(path).data).asInstanceOf[T]

  def forceUpdate(path: String, value: AnyRef) = forceUpdateData(path, zkClient.zkSerializer.serialize(value))

  def stopListenAll() = {
    zkClient.unsubscribeAll()
  }

  def reconnect() = zkClient.reconnect()

  private def deleteFailed(key: String): ToStorageException = {
    case e: Exception ⇒ CoordinationClient.deleteFailed(key, e)
  }

  private def deleteRecursiveFailed(key: String): ToStorageException = {
    case e: Exception ⇒ CoordinationClient.deleteRecursiveFailed(key, e)
  }

  private def writeDataFailed(key: String): ToStorageException = {
    case e: KeeperException.BadVersionException ⇒ CoordinationClient.writeDataFailedBadVersion(key, e)
    case e: ZkBadVersionException               ⇒ CoordinationClient.writeDataFailedBadVersion(key, e)
    case e: KeeperException.NoNodeException     ⇒ CoordinationClient.writeDataFailedMissingData(key, e)
    case e: ZkNoNodeException                   ⇒ CoordinationClient.writeDataFailedMissingData(key, e)
    case e: KeeperException                     ⇒ CoordinationClient.writeDataFailed(key, e)
  }

  private def readDataFailed(key: String): ToStorageException = {
    case e: KeeperException.NoNodeException ⇒ CoordinationClient.readDataFailedMissingData(key, e)
    case e: ZkNoNodeException               ⇒ CoordinationClient.readDataFailedMissingData(key, e)
    case e: KeeperException                 ⇒ CoordinationClient.readDataFailed(key, e)
  }

  private def existsFailed(key: String): ToStorageException = {
    case e: KeeperException ⇒ CoordinationClient.existsFailed(key, e)
  }

  private def createFailed(key: String): ToStorageException = {
    case e: KeeperException.NodeExistsException ⇒ CoordinationClient.createFailedDataExists(key, e)
    case e: ZkNodeExistsException               ⇒ CoordinationClient.createFailedDataExists(key, e)
    case e: KeeperException                     ⇒ CoordinationClient.createFailed(key, e)
  }

}

class ZookeeperCoordinationNodeListener(listener: CoordinationNodeListener) extends IZkChildListener {
  def handleChildChange(path: String, children: JList[String]) {
    listener.handleChange(path, List(children: _*))
  }
}

class ZookeeperCoordinationConnectionListener(listener: CoordinationConnectionListener) extends IZkStateListener {
  def handleNewSession() {
    listener.handleEvent(NewSession)
  }

  def handleStateChanged(state: Event.KeeperState) {
    state match {
      case KeeperState.SyncConnected ⇒
        listener.handleEvent(ThisNode.Connected)
      case KeeperState.Disconnected ⇒
        listener.handleEvent(ThisNode.Disconnected)
      case KeeperState.Expired ⇒
        listener.handleEvent(ThisNode.Expired)
    }
  }
}

class ZookeeperLockListener(listener: CoordinationLockListener) extends org.apache.zookeeper.recipes.lock.LockListener {
  def lockAcquired() {
    listener.lockAcquired()
  }

  def lockReleased() {
    listener.lockReleased()
  }
}

class ZookeeperCoordinationLock(zlock: WriteLock) extends CoordinationLock {
  def getId: String = zlock.getId

  def isOwner: Boolean = zlock.isOwner

  def lock(): Boolean = zlock.lock()

  def unlock() {
    zlock.unlock()
  }
}

class ZookeeperCoordinationClientFactory extends CoordinationClientFactory {
  //todo rename to cluster.zookeeper?
  val sessionTimeout = Duration(config.getInt("akka.cluster.session-timeout", 60), TIME_UNIT).toMillis.toInt
  val connectionTimeout = Duration(config.getInt("akka.cluster.connection-timeout", 60), TIME_UNIT).toMillis.toInt

  def createClient(servers: String) = {
    new ZookeeperCoordinationClient(new AkkaZkClient(servers, sessionTimeout, connectionTimeout))
  }
}

