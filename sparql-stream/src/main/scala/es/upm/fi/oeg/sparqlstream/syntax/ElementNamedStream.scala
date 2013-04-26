package es.upm.fi.oeg.sparqlstream.syntax

import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.graph.Node

class ElementNamedStream(node:Node,element:Element,window:ElementWindow) 
  extends ElementNamedGraph(node,element){
  
}