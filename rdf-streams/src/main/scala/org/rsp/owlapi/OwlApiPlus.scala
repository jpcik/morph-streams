package org.rsp.owlapi

import org.semanticweb.owlapi.model._
import eu.trowl.owlapi3.rel.tms.reasoner.dl.RELReasoner
import collection.JavaConversions._

object OwlApiTips{
  implicit class TrowlRelReasoner(reasoner:RELReasoner){
    def += (axiom:OWLAxiom)=
      reasoner.add(Set(axiom))
  }
  
  implicit class OwlClassPlus(theClass:OWLClass){
    def subClassOf(superclass:OWLClass)(implicit fac:OWLDataFactory)=   
      fac.getOWLSubClassOfAxiom(theClass, superclass)
  }
  implicit class OwlOntologyPlus(onto:OWLOntology){
    def += (axiom:OWLAxiom)(implicit mgr:OWLOntologyManager)=
      mgr.addAxiom(onto, axiom)
  }
  implicit class OwlIndividualPlus(ind:OWLIndividual){
    def ofClass (theclass:OWLClass)(implicit fac:OWLDataFactory)=
      fac.getOWLClassAssertionAxiom(theclass, ind)
  }
  
  implicit def str2Iri(s:String):IRI=IRI.create(s)
  object clazz{
    def apply(iri:String)(implicit fac:OWLDataFactory)=
      fac.getOWLClass(iri)

  }
  object ind{
    def apply(iri:String)(implicit fac:OWLDataFactory)=      
      fac.getOWLNamedIndividual(iri)
       
  }
  
}

