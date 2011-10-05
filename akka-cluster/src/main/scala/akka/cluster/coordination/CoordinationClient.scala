package akka.cluster.coordination

import akka.cluster.ChangeListener.ChangeNotification
import scala.PartialFunction
import akka.cluster.storage._

trait CoordinationClient {

  type ToStorageException = PartialFunction[Exception, StorageException]

  def serverAddresses: String

  def createData(path: String, value: Array[Byte])

  def createEphemeralData(path: String, value: Array[Byte])

  def createEphemeralSequentialData(path: String, value: Array[Byte]): String

  def readData(path: String): VersionedData

  def readData(path: String, version: Long): VersionedData

  def updateData(path: String, value: Array[Byte], expectedVersion: Long): VersionedData

  def forceUpdateData(path: String, value: Array[Byte]): VersionedData

  def exists(path: String): Boolean

  def createPath(path: String)

  def createEphemeralPath(path: String)

  def delete(path: String): Boolean

  def deleteRecursive(path: String): Boolean

  def create(path: String, value: AnyRef)

  def createEphemeral(path: String, value: AnyRef)

  def createEphemeralSequential(path: String, value: AnyRef): String

  def read[T](path: String): T

  def readWithVersion[T](path: String): (T, Long)

  def update(path: String, value: AnyRef, version: Long)

  def forceUpdate(path: String, value: AnyRef)

  def getChildren(path: String): List[String]

  def listenTo(path: String, listener: CoordinationNodeListener)

  def stopListenTo(path: String, listener: CoordinationNodeListener)

  def listenToConnection(listener: CoordinationConnectionListener)

  def stopListenToConnection(listener: CoordinationConnectionListener)

  def stopListenAll()

  def retryUntilConnected[T](code: ⇒ T): T

  def reconnect()

  def close()

  def getLock(path: String, listener: CoordinationLockListener): CoordinationLock

  val defaultStorageException: ToStorageException = {
    case s: StorageException   ⇒ s
    case underlying: Exception ⇒ new StorageException("Unexpected exception from the underlying storage impl", underlying)
  }

  def handleWith[T](exFunk: ToStorageException)(funk: ⇒ T): T = {
    try {
      funk
    } catch {
      case e: Exception ⇒ {
        val storageEx = exFunk orElse defaultStorageException
        throw storageEx(e)
      }
    }
  }

  def handle[T](funk: ⇒ T): T = {
    try {
      funk
    } catch {
      case e: Exception ⇒
        throw defaultStorageException(e)
    }
  }

}

object CoordinationClient {
  def existsFailed(key: String, underlying: Exception) = new StorageException(String.format("Failed to check existance for key [%s]", key), underlying)

  def createFailedDataExists(key: String, underlying: Exception): StorageException = new DataExistsException(String.format("Failed to insert key [%s]: an entry already exists with the same key", key), underlying)

  def createFailed(key: String, underlying: Exception) = new StorageException(String.format("Failed to insert key [%s]", key), underlying)

  def readDataFailed(key: String, underlying: Exception) = new StorageException(String.format("Failed to load key [%s]", key), underlying)

  def readDataFailedMissingData(key: String, underlying: Exception) = new MissingDataException(String.format("Failed to load key [%s]: no data was found", key), underlying)

  def readDataFailedBadVersion(key: String, expectedVersion: Long, actualVersion: Long) = new BadVersionException("Failed to load key [" + key + "]: version mismatch, expected [" + expectedVersion + "]" + " but found [" + actualVersion + "]")

  def writeDataFailed(key: String, underlying: Exception) = new StorageException(String.format("Failed to update key [%s]", key), underlying)

  def writeDataFailedBadVersion(key: String, underlying: Exception) = new BadVersionException(String.format("Failed to update key [%s]: version mismatch", key), underlying)

  def writeDataFailedMissingData(key: String, underlying: Exception) = new MissingDataException(String.format("Failed to update key [%s]: no data was found", key), underlying)

  def deleteFailed(key: String, underlying: Exception) = new StorageException(String.format("Failed to delete key [%s]", key), underlying)

  def deleteRecursiveFailed(key: String, underlying: Exception) = new StorageException(String.format("Failed to recursively delete path [%s]", key), underlying)
}

trait CoordinationNodeListener {

  def handleChange(path: String, children: List[String])

}

trait CoordinationConnectionListener {

  def handleEvent(event: ChangeNotification)

}

trait CoordinationSerializer {
  def deserialize(bytes: Array[Byte]): Any

  def serialize(serializable: Any): Array[Byte]
}

trait CoordinationLockListener {

  def lockAcquired()

  def lockReleased()

}

trait CoordinationLock {

  def getId: String

  def isOwner: Boolean

  def lock(): Boolean

  def unlock()

}

trait CoordinationClientFactory {

  def createClient(servers: String): CoordinationClient

}

