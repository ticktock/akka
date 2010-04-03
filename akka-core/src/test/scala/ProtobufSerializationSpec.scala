/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.actor

import org.scalatest.Spec
import org.scalatest.Assertions
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import se.scalablesolutions.akka.serialization.SerializerFactory
/*
@RunWith(classOf[JUnitRunner])
class ProtobufSerializationSpec extends
  Spec with 
  ShouldMatchers with 
  BeforeAndAfterAll {  

  describe("Protobuf serializer") {

    it("should be able to unserialize what it just serialized") {
      val factory = new SerializerFactory
      val pojo1 = ProtobufProtocol.ProtobufPOJO.getDefaultInstance.toBuilder.setId(1).setName("protobuf").setStatus(true).build

      val bytes = factory.getProtobuf.out(pojo1)
      val obj = factory.getProtobuf.in(bytes, pojo1.getClass)

      obj.isInstanceOf[ProtobufProtocol.ProtobufPOJO] should equal(true)
      val pojo2 = obj.asInstanceOf[ProtobufProtocol.ProtobufPOJO]
      pojo2.getId should equal(pojo1.getId)
      pojo1.getName should equal(pojo2.getName)
      pojo1.getStatus should equal(pojo2.getStatus)
    }
    
    it("should be able to do a deep clone of an object") {
      val factory = new SerializerFactory
      val pojo1 = ProtobufProtocol.ProtobufPOJO.getDefaultInstance.toBuilder.setId(1).setName("protobuf").setStatus(true).build

      val obj = factory.getProtobuf.deepClone(pojo1)

      obj.isInstanceOf[ProtobufProtocol.ProtobufPOJO] should equal(true)
      val pojo2 = obj.asInstanceOf[ProtobufProtocol.ProtobufPOJO]
      pojo2.getId should equal(pojo1.getId)
      pojo1.getName should equal(pojo2.getName)
      pojo1.getStatus should equal(pojo2.getStatus)
    }
  }
}
*/