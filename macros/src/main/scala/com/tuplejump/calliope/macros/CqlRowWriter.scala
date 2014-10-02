/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.tuplejump.calliope.macros

import scala.language.experimental.macros
import scala.reflect.macros.Context

import scala.language.implicitConversions
import com.tuplejump.calliope.Types.CQLRowMap


trait CqlRowWriterBase[T] extends Serializable {
  implicit def toCqlRow(t: T): CQLRowMap
}


object CqlRowWriter {

  import CqlRowEncodeMacro._

  def lowercaseMapper[T] = macro _generateWithLowercaseMapper[T]

  def snakecaseMapper[T] = macro _generateWithSnakecaseMapper[T]

  def columnListMapper[T](columns: String*) = macro _generateWithColumnList[T]

  def columnMapper[T](columnMap: Map[String, String]) = macro _generateWithColumnMap[T]

  def functionMapper[T](columnMapper: (String, Int) => String) = macro _generateWithColumnMapper[T]
}

object CqlRowEncodeMacro {

  def _generateWithColumnList[T: c.WeakTypeTag](c: Context)(columns: c.Expr[String]*): c.Expr[CqlRowWriterBase[T]] = {

    val tpe = c.weakTypeOf[T]

    ensureCaseClass(c)(tpe)

    val params = getParams(c)(tpe)

    if (!columns.isEmpty && (columns.length != params.length)) {
      c.abort(c.enclosingPosition, s"The case class [${tpe}}] expects ${params.length} values, you provided ${columns.length}")
    }

    val cols = columns.map(col => c.eval(c.Expr[String](c.resetAllAttrs(col.tree))))

    def colMapper: (String, Int) => String = {
      (s: String, i: Int) => cols(i)
    }

    generate[T, c.type](c)(tpe, params, colMapper)
  }

  def _generateWithColumnMapper[T: c.WeakTypeTag](c: Context)(columnMapper: c.Expr[(String, Int) => String]): c.Expr[CqlRowWriterBase[T]] = {
    val tpe = c.weakTypeOf[T]

    ensureCaseClass(c)(tpe)

    val params = getParams(c)(tpe)

    val colMapper: (String, Int) => String = c.eval(c.Expr[(String, Int) => String](c.resetAllAttrs(columnMapper.tree)))

    generate[T, c.type](c)(tpe, params, colMapper)
  }

  def _generateWithColumnMap[T: c.WeakTypeTag](c: Context)(columnMap: c.Expr[Map[String, String]]): c.Expr[CqlRowWriterBase[T]] = {
    import c.universe._
    val colMap: Map[String, String] = c.eval(c.Expr[Map[String, String]](c.resetAllAttrs(columnMap.tree)))
    val colMapper = reify {
      (s: String, i: Int) => colMap.getOrElse(s, s)
    }

    _generateWithColumnMapper[T](c)(colMapper)
  }

  def _generateWithLowercaseMapper[T: c.WeakTypeTag](c: Context): c.Expr[CqlRowWriterBase[T]] = {
    import c.universe._

    val colMapper = reify {
      (s: String, i: Int) => s.toLowerCase
    }
    _generateWithColumnMapper[T](c)(colMapper)
  }

  def _generateWithSnakecaseMapper[T: c.WeakTypeTag](c: Context): c.Expr[CqlRowWriterBase[T]] = {
    import c.universe._


    val colMapper = reify {
      (s: String, i: Int) => s.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase
    }

    _generateWithColumnMapper[T](c)(colMapper)
  }

  private def generate[T, C <: Context](c: C)(tpe: c.Type, params: List[c.universe.Symbol], mapperFunction: (String, Int) => String): c.Expr[CqlRowWriterBase[T]] = {

    //The Next line should be (import c.universe._) Idea has an habit of removing it as unused import
    import c.universe._
    val companion: Symbol = tpe.typeSymbol.companionSymbol //DO NOT REMOVE THIS LINE

    val toMapParams = getMappers(c)(params, mapperFunction)

    c.Expr[CqlRowWriterBase[T]] {
      q"""
      import com.tuplejump.calliope.macros.CqlRowWriterBase
      import com.tuplejump.calliope.utils.RichByteBuffer._
      import java.nio.ByteBuffer

      new CqlRowWriterBase[$tpe] {
        import com.tuplejump.calliope.Types.CQLRowMap

        implicit def toCqlRow(t: $tpe):CQLRowMap = Map(..$toMapParams)
      }
    """
    }
  }


  private def ensureCaseClass(c: Context)(tpe: c.Type) {
    val sym = tpe.typeSymbol
    if (!sym.isClass || !sym.asClass.isCaseClass)
      c.abort(c.enclosingPosition, s"$sym is not a case class")
  }

  private def getParams(c: Context)(tpe: c.Type) = {

    import c.universe._

    val declarations = tpe.declarations

    val ctor = declarations.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m
    }.get

    ctor.paramss.head

  }


  private def getMappers(c: Context)(params: List[c.universe.Symbol], mapperFunction: (String, Int) => String) = {

    import c.universe._

    params.zipWithIndex.map {
      case (field: Symbol, index: Int) =>
        val colName = mapperFunction(field.name.toString, index)
        val name = field.name.asInstanceOf[TermName]
        val fieldType: Type = field.asTerm.typeSignature

        val toMap = q"""
          val trans = implicitly[$fieldType => ByteBuffer]
          $colName -> t.$name
        """

        toMap
    }
  }

  implicit def ss2sis(ss: String => String): (String, Int) => String = {
    (s: String, i: Int) => ss(s)
  }

  implicit def is2sis(is: String => String): (String, Int) => String = {
    (s: String, i: Int) => is(s)
  }
}
