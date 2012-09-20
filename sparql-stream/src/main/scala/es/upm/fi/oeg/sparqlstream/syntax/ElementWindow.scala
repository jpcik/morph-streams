package es.upm.fi.oeg.sparqlstream.syntax
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap
import com.hp.hpl.jena.sparql.syntax.ElementVisitor

class ElementWindow extends Element{
  override def equalTo(el2:Element, isoMap:NodeIsomorphismMap)=false
  override def hashCode() =0
  override def visit(v:ElementVisitor ) {}
}

class ElementTimeWindow(val from:ElementTimeValue,
    val to:ElementTimeValue,val slide:ElementTimeValue) extends ElementWindow {
}


class ElementTripleWindow(val range:Int,val delta:Int) extends ElementWindow {  
}