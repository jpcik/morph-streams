package es.upm.fi.oeg.sparqlstream

import com.hp.hpl.jena.query.Query
import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph
import scala.collection.mutable.ArrayBuffer

class StreamQuery extends Query{
  val streams = new collection.mutable.HashMap[String,ElementStreamGraph]
  val r2s = new ArrayBuffer[String]
  def isRstream=r2s.contains("rstream")
  def isDstream=r2s.contains("dstream")
  def isIstream=r2s.contains("istream")
}