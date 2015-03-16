package org.rsp.jena

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model._
import com.hp.hpl.jena.query._

object JenaPlus {
  
  object sparql{
    def apply(qs:String)=QueryFactory.create(qs)    
  }
  implicit class QueryPlus(q:Query){
    def select(m:Model)=
      QueryExecutionFactory.create(q, m).execSelect
    def serviceSelect(service:String)=
      QueryExecutionFactory.sparqlService(service, q).execSelect
  }
  def lit(v:String)(implicit qs:QuerySolution)=
    qs.getLiteral(v)
  def res(v:String)(implicit qs:QuerySolution)=
    qs.getResource(v)
  
  implicit class QuerysolutionPlus(qs:QuerySolution){
    def lito(v:String)=qs.getLiteral(v)
  }
  
  implicit class TriplePlus(t:Triple){
    //def ofClass (theclass:OWLClass)(implicit fac:OWLDataFactory)=
      //fac.getOWLClassAssertionAxiom(theclass, ind)
  }
  object uri{
    def apply(iri:String)=NodeFactory.createURI(iri)
  }
  implicit def str2uri(str:String)=uri(str)
    //implicit def str2res(str:String)=ResourceFactory.createResource(str)
  def bnode()(implicit m:Model)=m.createResource()
  def bnode(id:String)(implicit m:Model)=m.createResource(new AnonId(id))
  def iri(uri:String)(implicit m:Model):Resource=m.createResource(uri)
  def add(uri:String,po:(Property,Any))(implicit m:Model):Resource={
    val res= iri(uri)
    res.+(po)
    res
  }
  def add(uri:String,pos:(Property,Any)*)(implicit m:Model):Resource={
    val res= iri(uri)
    pos foreach{po=>
      res.+(po)
    }
    res
  }

  def add(res:Resource,pos:(Property,Any)*)(implicit m:Model):Resource={    
    //val res= iri(res.getURI)
    pos foreach{po=>
      res.+(po)
    }
    res
  }

  
  implicit def double2Literal(d:Double)=NodeFactory.createLiteral(d.toString)
  implicit def prop2node(prop:Property)=prop.asNode
  implicit def str2res(str:String)(implicit m:Model)=m.createResource(str)
  def t(s:Resource,p:Property,o:RDFNode)(implicit m:Model)=m.createStatement(s,p,o)
  def t(s:Resource,p:Property,o:String)(implicit m:Model)=m.createStatement(s,p,o)
  implicit class ResourceMore(r:Resource){
    def prop(p:Property,v:String)=r.addProperty(p, v)
    def prop(p:Property,v:Resource)=r.addProperty(p, v)
    def ~(p:Property,v:String)=r.addProperty(p, v)
    def ~(p:Property,v:Resource)=prop(p, v)
    def +(pos:(Property,Any)*)=
      pos foreach{
      case (p,s:String)=>r.addProperty(p, s)
      case (p,s:Resource)=>r.addProperty(p, s)
    }
    
  }
  implicit class StatementMore(s:Statement){
    def sub=s.getSubject()
    def pred=s.getPredicate()
    def obj=s.getObject()
    
  }
      
  
  object Iri{
    def unapply(node:RDFNode)=
      if (node.isURIResource) Some(node.asResource.getURI) else None
  }
  
  object Bnode{    
    def unapply(node:RDFNode)=
      if (node.isAnon) Some(node.asResource.getId) else None
  }
  
  implicit class ModelMore(m:Model){
    
    //def res(s:String)=m.createResource(s)
  }
}