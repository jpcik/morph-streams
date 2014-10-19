package org.rsp

import eu.trowl.owlapi3.rel.tms.reasoner.dl.RELReasonerFactory
import collection.JavaConversions._
import org.rsp.owlapi.OwlApiTips._
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLAxiom


object StreamTrowl extends DemoData {
  def main(args:Array[String]):Unit={
    implicit val mgr=OWLManager.createOWLOntologyManager
    implicit val fac=mgr.getOWLDataFactory
    val onto=mgr.createOntology
    
    val superSystem=Class(rspEx+"SuperSystem")
    val system=Class(sswObs +"System")    
    onto += (system subClassOf superSystem )
    
    val reasoner = new RELReasonerFactory().createReasoner(onto)
    
    (1 to 10).foreach{i=>
      reasoner += (Indiv(rspEx+"sys"+i) ofClass system)
      reasoner.reclassify
    }
    println (reasoner.getInstances(superSystem, true).size)

    
  }
}