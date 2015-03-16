package org.rsp.rdf

import java.net.URI

object Rdf{
trait RdfTerm
class Literal(value:String,datatype:Iri,langTag:String) extends RdfTerm
implicit class Iri(value:String) extends RdfTerm{
  //val prefix=URI.create(value).
}
object b{
def apply(vali:String)=Bnode(vali)
}
case class Bnode(id:String) extends RdfTerm{
  
}


case class RdfTriple(subject:RdfTerm,predicate:Iri,_object:RdfTerm){
  val s=subject
}

object t{
  def apply(s:RdfTerm,p:Iri,o:RdfTerm)=RdfTriple(s,p,o)
}
implicit def tuple2Triple(t:Tuple3[RdfTerm,Iri,RdfTerm])=RdfTriple(t._1 ,t._2,t._3)
implicit def tuple2Tripl(t:(RdfTerm,String,String))=RdfTriple(t._1 ,t._2,t._3)

class RdfGraph(triples:Set[RdfTriple])

}

class Test{
  import Rdf._
   t ("dfsfs","","")
   
}