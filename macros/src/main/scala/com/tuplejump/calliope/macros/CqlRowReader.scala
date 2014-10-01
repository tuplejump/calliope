package com.tuplejump.calliope.macros

import scala.language.experimental.macros
import scala.reflect.macros.Context

import scala.language.implicitConversions
import com.tuplejump.calliope.Types.{CQLRowKeyMap, CQLRowMap}

trait CqlRowReaderBase[T] extends Serializable {
  implicit def fromCqlRow(k: CQLRowKeyMap, v: CQLRowMap): T
}

object CqlRowReader {

  import CqlRowDecodeMacro._

  def lowercaseMapper[T] = macro _generateWithLowercaseMapper[T]

  def snakecaseMapper[T] = macro _generateWithSnakecaseMapper[T]

  def columnListMapper[T](columns: String*) = macro _generateWithColumnList[T]

  def columnMapper[T](columnMap: Map[String, String]) = macro _generateWithColumnMap[T]

  def functionMapper[T](columnMapper: (String, Int) => String) = macro _generateWithColumnMapper[T]
}

object CqlRowDecodeMacro {

  def _generateWithColumnList[T: c.WeakTypeTag](c: Context)(columns: c.Expr[String]*): c.Expr[CqlRowReaderBase[T]] = {

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

  def _generateWithColumnMapper[T: c.WeakTypeTag](c: Context)(columnMapper: c.Expr[(String, Int) => String]): c.Expr[CqlRowReaderBase[T]] = {
    val tpe = c.weakTypeOf[T]

    ensureCaseClass(c)(tpe)

    val params = getParams(c)(tpe)

    val colMapper: (String, Int) => String = c.eval(c.Expr[(String, Int) => String](c.resetAllAttrs(columnMapper.tree)))

    generate[T, c.type](c)(tpe, params, colMapper)
  }

  def _generateWithColumnMap[T: c.WeakTypeTag](c: Context)(columnMap: c.Expr[Map[String, String]]): c.Expr[CqlRowReaderBase[T]] = {
    import c.universe._
    val colMap: Map[String, String] = c.eval(c.Expr[Map[String, String]](c.resetAllAttrs(columnMap.tree)))
    val colMapper = reify {
      (s: String, i: Int) => colMap.getOrElse(s, s)
    }

    _generateWithColumnMapper[T](c)(colMapper)
  }

  def _generateWithLowercaseMapper[T: c.WeakTypeTag](c: Context): c.Expr[CqlRowReaderBase[T]] = {
    import c.universe._

    val colMapper = reify {
      (s: String, i: Int) => s.toLowerCase
    }
    _generateWithColumnMapper[T](c)(colMapper)
  }

  def _generateWithSnakecaseMapper[T: c.WeakTypeTag](c: Context): c.Expr[CqlRowReaderBase[T]] = {
    import c.universe._


    val colMapper = reify {
      (s: String, i: Int) => s.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase
    }

    _generateWithColumnMapper[T](c)(colMapper)
  }

  private def generate[T, C <: Context](c: C)(tpe: c.Type, params: List[c.universe.Symbol], mapperFunction: (String, Int) => String): c.Expr[CqlRowReaderBase[T]] = {
    import c.universe._

    val companion: Symbol = tpe.typeSymbol.companionSymbol
    val fromMapParams = getMappers(c)(params, mapperFunction)

    c.Expr[CqlRowReaderBase[T]] {
      q"""
      import com.tuplejump.calliope.macros.CqlRowReaderBase

      new CqlRowReaderBase[$tpe] {
        import com.tuplejump.calliope.Types.{CQLRowKeyMap, CQLRowMap}
        import com.tuplejump.calliope.utils.RichByteBuffer._
        import java.nio.ByteBuffer

        implicit def fromCqlRow(k: CQLRowKeyMap, v: CQLRowMap) = {
          val map = k ++ v
          $companion(..$fromMapParams)
        }
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
        val fieldType: Type = field.asTerm.typeSignature
        val colName = mapperFunction(field.name.toString, index)

        q"""
            val trans = implicitly[ByteBuffer => $fieldType]
            trans(map($colName))
         """
    }
  }

  implicit def ss2sis(ss: String => String): (String, Int) => String = {
    (s: String, i: Int) => ss(s)
  }

  implicit def is2sis(is: String => String): (String, Int) => String = {
    (s: String, i: Int) => is(s)
  }
}
