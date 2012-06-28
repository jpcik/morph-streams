package es.upm.fi.oeg.morph.stream.query
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp

trait SourceQuery {
  def load(op:AlgebraOp)
  def serializeQuery:String
  def supportsPostProc:Boolean
  def getProjection:Map[String, String] 
}