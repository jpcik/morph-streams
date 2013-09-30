package es.upm.fi.oeg.sparqlstream

import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.algebra.optimize.TransformSimplify
import com.hp.hpl.jena.sparql.algebra.Transformer
import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph
import com.hp.hpl.jena.sparql.algebra.op.OpGraph
import es.upm.fi.oeg.sparqlstream.syntax.ElementNamedStream
import com.hp.hpl.jena.graph.Node
import es.upm.fi.oeg.sparqlstream.syntax.ElementWindow

class StreamAlgebraGenerator extends AlgebraGenerator {
  private val applySimplification = true  
  private val simplifyTooEarlyInAlgebraGeneration = false    
  private val simplify = new TransformSimplify

  override def compile(elt:Element):Op={
    val op = compileElement(elt)
    if ( ! simplifyTooEarlyInAlgebraGeneration && applySimplification &&  simplify != null )
       simplify(op)
    else op
  }
  
  override def compileElement(elt:Element):Op=elt match {
    case streamgraph:ElementNamedStream=>compileElementStreamGraph(streamgraph)
    case _ => super.compileElement(elt)
  }
  
  protected def compileElementStreamGraph(eltGraph:ElementNamedStream):Op={
    val sub = compileElement(eltGraph.getElement) 
    new OpStreamGraph(eltGraph.getGraphNameNode,sub,eltGraph.window) 
  }
  
  private def simplify(op:Op):Op=Transformer.transform(simplify, op)     
}

class OpStreamGraph(node:Node,op:Op,window:ElementWindow) extends OpGraph(node,op){
  override def copy(newOp:Op)=new OpStreamGraph(node, newOp,window)
  override def getName()="stream-graph"
}