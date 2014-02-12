package es.upm.fi.oeg.morph.stream.query
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import com.hp.hpl.jena.sparql.syntax.Template

abstract class SourceQuery(op:AlgebraOp) {
  protected def build(op:AlgebraOp):String
  def serializeQuery:String
  def supportsPostProc:Boolean
  def projectionVars:Array[String]
  def construct:Template
  def create:InstQuery
}

object Modifiers{
  trait OutputModifier
  object Rstream extends OutputModifier
  object Istream extends OutputModifier
  object Dstream extends OutputModifier
}