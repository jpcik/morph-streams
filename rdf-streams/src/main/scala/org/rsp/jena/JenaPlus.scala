package org.rsp.jena

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.rdf.model.Property

object JenaPlus {
  implicit class TriplePlus(t:Triple){
    //def ofClass (theclass:OWLClass)(implicit fac:OWLDataFactory)=
      //fac.getOWLClassAssertionAxiom(theclass, ind)
  }
  object uri{
    def apply(iri:String)=NodeFactory.createURI(iri)
  }
  implicit def str2uri(str:String)=uri(str)
  implicit def double2Literal(d:Double)=NodeFactory.createLiteral(d.toString)
  implicit def prop2node(prop:Property)=prop.asNode
}