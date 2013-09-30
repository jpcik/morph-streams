package es.upm.fi.oeg.sparqlstream.syntax

import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.graph.Node

class ElementNamedStream(val node:Node,val element:Element,val window:ElementWindow) 
  extends ElementNamedGraph(node,element){
  
}