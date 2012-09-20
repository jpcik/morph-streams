package es.upm.fi.oeg.sparqlstream.syntax
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap
import com.hp.hpl.jena.sparql.syntax.ElementVisitor
import scala.reflect.BeanProperty
import es.upm.fi.oeg.morph.common.TimeUnit

class ElementTimeValue(val time:Long,@BeanProperty val unit:TimeUnit) extends Element{
  override def equalTo(el2:Element, isoMap:NodeIsomorphismMap) =
	el2 match{
	  case el2:ElementTimeValue=>el2.time==time && el2.unit==unit
	  case _=> false
    }
  
  override def hashCode() =0;
 
  override def visit(v:ElementVisitor) {
		// TODO Auto-generated method stub
  }	
}
