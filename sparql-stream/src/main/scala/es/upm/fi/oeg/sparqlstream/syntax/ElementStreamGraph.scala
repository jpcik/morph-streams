package es.upm.fi.oeg.sparqlstream.syntax
import com.hp.hpl.jena.sparql.syntax.ElementVisitor
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap
import com.hp.hpl.jena.sparql.syntax.Element
import scala.reflect.BeanProperty

class ElementStreamGraph(@BeanProperty val uri:String,
    @BeanProperty val window:ElementWindow) extends Element{
  override def equalTo(el2:Element,isoMap:NodeIsomorphismMap)=false
  override def hashCode() =	0
  override def visit(v:ElementVisitor) {}
}