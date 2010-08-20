package se.scalablesolutions.akka.persistence.redis

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import se.scalablesolutions.akka.actor.{Actor}
import se.scalablesolutions.akka.config.OneForOneStrategy
import Actor._
import se.scalablesolutions.akka.persistence.common.PersistentVector
import se.scalablesolutions.akka.stm.global._
import se.scalablesolutions.akka.config.ScalaConfig._
import se.scalablesolutions.akka.util.Logging

import RedisStorageBackend._

case class GET(k: String)
case class SET(k: String, v: String)
case class REM(k: String)
case class CONTAINS(k: String)
case object MAP_SIZE
case class MSET(kvs: List[(String, String)])
case class REMOVE_AFTER_PUT(kvsToAdd: List[(String, String)], ksToRem: List[String])
case class CLEAR_AFTER_PUT(kvsToAdd: List[(String, String)])
case class PUT_WITH_SLICE(kvsToAdd: List[(String, String)], start: String, cnt: Int)
case class PUT_REM_WITH_SLICE(kvsToAdd: List[(String, String)], ksToRem: List[String], start: String, cnt: Int)

object Storage {
  class RedisSampleStorage extends Actor {
    self.lifeCycle = Some(LifeCycle(Permanent))
    val FOO_MAP = "akka.sample.map"

    private var fooMap = atomic { RedisStorage.getMap(FOO_MAP) }

    def receive = {
      case SET(k, v) =>
        atomic {
          fooMap += (k.getBytes, v.getBytes)
        }
        self.reply((k, v))

      case GET(k) =>
        val v = atomic {
          fooMap.get(k.getBytes)
        }
        self.reply(v.collect {case byte => new String(byte)}.getOrElse(k + " Not found"))

      case REM(k) =>
        val v = atomic {
          fooMap -= k.getBytes
        }
        self.reply(k)

      case CONTAINS(k) =>
        val v = atomic {
          fooMap contains k.getBytes
        }
        self.reply(v)

      case MAP_SIZE =>
        val v = atomic {
          fooMap.size
        }
        self.reply(v)

      case MSET(kvs) => 
        atomic {
          kvs.foreach {kv =>
            fooMap += (kv._1.getBytes, kv._2.getBytes)
          }
        }
        self.reply(kvs.size)

      case REMOVE_AFTER_PUT(kvs2add, ks2rem) => 
        val v =
          atomic {
            kvs2add.foreach {kv =>
              fooMap += (kv._1.getBytes, kv._2.getBytes)
            }
  
            ks2rem.foreach {k =>
              fooMap -= k.getBytes
            }
            fooMap.size
          }
        self.reply(v)

      case CLEAR_AFTER_PUT(kvs2add) => 
        atomic {
          kvs2add.foreach {kv =>
            fooMap += (kv._1.getBytes, kv._2.getBytes)
          }
          fooMap.clear
        }
        self.reply(true)

      case PUT_WITH_SLICE(kvs2add, from, cnt) => 
        val v = 
          atomic {
            kvs2add.foreach {kv =>
              fooMap += (kv._1.getBytes, kv._2.getBytes)
            }
            fooMap.slice(Some(from.getBytes), cnt)
          }
        self.reply(v: List[(Array[Byte], Array[Byte])])

      case PUT_REM_WITH_SLICE(kvs2add, ks2rem, from, cnt) => 
        val v = 
          atomic {
            kvs2add.foreach {kv =>
              fooMap += (kv._1.getBytes, kv._2.getBytes)
            }
            ks2rem.foreach {k =>
              fooMap -= k.getBytes
            }
            fooMap.slice(Some(from.getBytes), cnt)
          }
        self.reply(v: List[(Array[Byte], Array[Byte])])
    }
  }
}

import Storage._

@RunWith(classOf[JUnitRunner])
class RedisTicket343Spec extends
  Spec with
  ShouldMatchers with
  BeforeAndAfterAll with
  BeforeAndAfterEach {

  override def beforeAll {
    flushDB
    println("** destroyed database")
  }

  override def afterEach {
    flushDB
    println("** destroyed database")
  }

  describe("Ticket 343 Issue #1") {
    it("remove after put should work within the same transaction") {
      val proc = actorOf[RedisSampleStorage]
      proc.start

      (proc !! SET("debasish", "anshinsoft")).getOrElse("Set failed") should equal(("debasish", "anshinsoft"))
      (proc !! GET("debasish")).getOrElse("Get failed") should equal("anshinsoft")
      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(1)

      (proc !! MSET(List(("dg", "1"), ("mc", "2"), ("nd", "3")))).getOrElse("Mset failed") should equal(3)

      (proc !! GET("dg")).getOrElse("Get failed") should equal("1")
      (proc !! GET("mc")).getOrElse("Get failed") should equal("2")
      (proc !! GET("nd")).getOrElse("Get failed") should equal("3")

      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(4)

      val add = List(("a", "1"), ("b", "2"), ("c", "3"))
      val rem = List("a", "debasish")
      (proc !! REMOVE_AFTER_PUT(add, rem)).getOrElse("REMOVE_AFTER_PUT failed") should equal(5)

      (proc !! GET("debasish")).getOrElse("debasish not found") should equal("debasish Not found")
      (proc !! GET("a")).getOrElse("a not found") should equal("a Not found")

      (proc !! GET("b")).getOrElse("b not found") should equal("2")

      (proc !! CONTAINS("b")).getOrElse("b not found") should equal(true)
      (proc !! CONTAINS("debasish")).getOrElse("debasish not found") should equal(false)
      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(5)
    }
  }

  describe("Ticket 343 Issue #2") {
    it("clear after put should work within the same transaction") {
      val pro = actorOf[RedisSampleStorage]
      pro.start

      (pro !! SET("debasish", "anshinsoft")).getOrElse("Set failed") should equal(("debasish", "anshinsoft"))
      (pro !! GET("debasish")).getOrElse("Get failed") should equal("anshinsoft")
      (pro !! MAP_SIZE).getOrElse("Size failed") should equal(1)

      val add = List(("a", "1"), ("b", "2"), ("c", "3"))
      (pro !! CLEAR_AFTER_PUT(add)).getOrElse("CLEAR_AFTER_PUT failed") should equal(true)

      val thrown = 
        evaluating {
          (pro !! MAP_SIZE).getOrElse("Size failed") should equal(1)
        } should produce [Exception]
    }
  }

  describe("Ticket 343 Issue #3") {
    it("map size should change after the transaction") {
      val proc = actorOf[RedisSampleStorage]
      proc.start

      (proc !! SET("debasish", "anshinsoft")).getOrElse("Set failed") should equal(("debasish", "anshinsoft"))
      (proc !! GET("debasish")).getOrElse("Get failed") should equal("anshinsoft")
      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(1)

      (proc !! MSET(List(("dg", "1"), ("mc", "2"), ("nd", "3")))).getOrElse("Mset failed") should equal(3)
      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(4)

      (proc !! GET("dg")).getOrElse("Get failed") should equal("1")
      (proc !! GET("mc")).getOrElse("Get failed") should equal("2")
      (proc !! GET("nd")).getOrElse("Get failed") should equal("3")
    }
  }

  describe("slice test") {
    it("should pass") {
      val proc = actorOf[RedisSampleStorage]
      proc.start

      (proc !! SET("debasish", "anshinsoft")).getOrElse("Set failed") should equal(("debasish", "anshinsoft"))
      (proc !! GET("debasish")).getOrElse("Get failed") should equal("anshinsoft")
      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(1)

      (proc !! MSET(List(("dg", "1"), ("mc", "2"), ("nd", "3")))).getOrElse("Mset failed") should equal(3)
      (proc !! MAP_SIZE).getOrElse("Size failed") should equal(4)

      (proc !! PUT_WITH_SLICE(List(("ec", "1"), ("tb", "2"), ("mc", "10")), "dg", 3)).get.asInstanceOf[List[(Array[Byte], Array[Byte])]].map { case (k, v) => (new String(k), new String(v)) } should equal(List(("dg", "1"), ("ec", "1"), ("mc", "10")))

      (proc !! PUT_REM_WITH_SLICE(List(("fc", "1"), ("gb", "2"), ("xy", "10")), List("tb", "fc"), "dg", 5)).get.asInstanceOf[List[(Array[Byte], Array[Byte])]].map { case (k, v) => (new String(k), new String(v)) } should equal(List(("dg", "1"), ("ec", "1"), ("gb", "2"), ("mc", "10"), ("nd", "3")))
    }
  }
}
