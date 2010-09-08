/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */
package se.scalablesolutions.akka.jersey

import se.scalablesolutions.akka.util.Logging

import scala.annotation.target.beanGetter

import scala.collection._
import generic._
import immutable.{List, HashSet, Vector, IndexedSeq}
import mutable.{HashSet => MutableHashSet}
import JavaConversions._

import javax.ws.rs.QueryParam
import javax.ws.rs.ext.Provider
import javax.ws.rs.core.MultivaluedMap
import com.sun.jersey.api.ParamException.{QueryParamException}
import com.sun.jersey.api.model.Parameter
import com.sun.jersey.api.core.HttpContext
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable
import com.sun.jersey.spi.inject.{Injectable, InjectableProvider}
import com.sun.jersey.core.spi.component.{ComponentContext, ComponentScope}
import com.sun.jersey.server.impl.model.parameter.multivalued.{MultivaluedParameterExtractor => MVPExtractor}

/** 
 * Jersey Injection provider which provides the ability 
 * to use QueryParams as Scala types (esp. when you stack 'em as collections)
 * 
 * Partially inspired by Coda Hale's jersey-scala code at http://github.com/codahale/jersey-scala
 *
 * @author Brendan W. McAdams <bmcadams@novus.com>
 * @version 1.0, 09/08/10
 * @since 1.0
 * 
 * @tparam QueryParam 
 * @tparam Parameter 
 */
@Provider
class ScalaQueryParamInjectableProvider 
  extends InjectableProvider[QueryParam, Parameter] with Logging {

  def getInjectable(cmpCtx: ComponentContext, queryParam: QueryParam, parameter: Parameter) = 
    new ScalaQueryParamInjectable(cmpCtx, queryParam, parameter)

  /* This component is scoped at the request level */
  def getScope = ComponentScope.PerRequest

}

class ScalaQueryParamInjectable protected[akka](cmpCtx: ComponentContext, queryParam: QueryParam, parameter: Parameter) 
  extends AbstractHttpContextInjectable[AnyRef] with Logging {

  val paramName = parameter.getSourceName
  require(paramName != null && !paramName.isEmpty, "Invalid Parameter Name")

  /** 
   * No matter what you ask for we try to give you an immutable version by default 
   * For the moment, everything inner type-wise is really a string as we can't work 
   * around type erasure
   * at runtime in a sane way.
   */
  val extractor = 
    if (parameter.getParameterClass == classOf[Vector[_]]) 
      new CollectionExtractor[Vector](paramName, parameter.getDefaultValue, Vector)
    else if (parameter.getParameterClass == classOf[IndexedSeq[_]]) 
      new CollectionExtractor[IndexedSeq](paramName, parameter.getDefaultValue, IndexedSeq)
    else if (parameter.getParameterClass == classOf[HashSet[_]])  
      new CollectionExtractor[HashSet](paramName, parameter.getDefaultValue, immutable.HashSet)
    else if (parameter.getParameterClass == classOf[List[_]])
      new CollectionExtractor[List](paramName, parameter.getDefaultValue, List)
    else if (parameter.getParameterClass == classOf[MutableHashSet[_]]) 
      new CollectionExtractor[MutableHashSet](paramName, parameter.getDefaultValue, MutableHashSet)
    else if (parameter.getParameterClass == classOf[Seq[_]]) 
      new CollectionExtractor[Seq](paramName, parameter.getDefaultValue, Seq)
    else if (parameter.getParameterClass == classOf[Set[_]]) 
      new CollectionExtractor[Set](paramName, parameter.getDefaultValue, Set)
    else if (parameter.getParameterClass == classOf[Option[_]])  
      new OptionExtractor(paramName, parameter.getDefaultValue)    
    else 
      null 
    /**
    * I believe we don't want to throw an exception as null is a 'pass', 
    * letting a default provider handle it
    */

  def getValue(httpCtx: HttpContext) = try {
    extractor extract(httpCtx.getUriInfo.getQueryParameters(!(parameter isEncoded)))
  } catch {
    case e => {
      log.error(e, "ScalaQueryParamInjectable failed to decode Query Parameter '%s' due to '%s'", paramName, e.toString)
      throw new QueryParamException(e.getCause, extractor.param, extractor.default)
    }
  }

}


protected[akka] trait ParamExtractor extends MVPExtractor {

  val param: String
  val default: String

  def getName = param

  def getDefaultStringValue = default

  def extract(params: MultivaluedMap[String, String]): AnyRef
    
}

protected[akka] class OptionExtractor(val param: String, val default: String) extends ParamExtractor {
  def extract(params: MultivaluedMap[String, String]) = 
    Option(params.getFirst(param)) orElse Option(default) // Option() will return None if null ... TODO - "" ?
}

protected[akka] class CollectionExtractor[+CC[X] <: Traversable[X]](val param: String, 
                                                                    val default: String, 
                                                                    val collType: GenericCompanion[CC]) extends ParamExtractor {

  def extract(params: MultivaluedMap[String, String]) = {

    val bldr = collType.newBuilder[String]
    params.get(param) match {
      case null => if (default != null) bldr += default
      case pL => {
        bldr.sizeHint(pL.size)
        bldr ++= pL
      }
    }
    bldr result
  }

}


// vim: set ts=2 sw=2 sts=2 et:
