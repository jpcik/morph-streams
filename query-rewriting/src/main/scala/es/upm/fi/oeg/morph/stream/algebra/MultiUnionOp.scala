package es.upm.fi.oeg.morph.stream.algebra
import org.apache.commons.lang.NotImplementedException
import scala.collection.immutable.TreeMap
import java.util.HashMap
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import org.slf4j.LoggerFactory

class MultiUnionOp(childrenOps:Map[String,AlgebraOp]) 
  extends AlgebraOp {

  private val logger= LoggerFactory.getLogger(this.getClass)
  override val name="multiunion"
  private val nodups=childrenOps
  val children=nodups.filter(_._2!=null)
  
  override val id={
    children.map(c=>c._2.id).mkString("-")
  }
  
  override def join(op:AlgebraOp):AlgebraOp=op match{
	case multi:MultiUnionOp=>
	  new InnerJoinOp(this, multi)	  
	case proj:ProjectionOp=>
	  new InnerJoinOp(this, proj)
	case _=> throw new NotImplementedException("Join for: "+op.toString)
  }

  override def copyOp:AlgebraOp={
	throw new NotImplementedException("we need to do this: ")
  }

  private def tab(level:Int)=(0 until level).map(_=>"\t").mkString  
	
  override def display(level:Int)	{
	logger.warn(tab(level)+name)
	children.values.take(30).foreach(op=>op.display(level+1))
  }

  override def display()={
	display(0)
  }
	
  override def vars=
	children.values.map(child=>child.vars).reduceLeft(_++_)
	
  override def merge(op:AlgebraOp,xprs:Seq[Xpr]):AlgebraOp=op match {
    case union:MultiUnionOp=>
	  logger.debug("merge unions: "+children.keySet+"--"+union.children.keySet)
		
	  val set =  this.children.keySet 			
	  val ops=set.map{key=>
		val child =  children(key)
		union.children.keySet.map{key2=>
		  val child2 = union.children(key2)
		  val opnew =child.copyOp.merge(child2.copyOp,xprs)					
		  key+key2->opnew
		}.filter(_._2!=null)
	  }.flatten.toMap	
	  new MultiUnionOp(ops)					
    case _ =>
		throw new NotImplementedException("Merge implementation missing: "+op.toString());
		
  }
  
  def simplify={
    //if (logger.isDebugEnabled)
      //this.display
    if (children.isEmpty) null
    else if (children.size==1) children.head._2
    else if (children.exists(c=>c._2.isInstanceOf[MultiUnionOp]))
      MultiUnionOp(children.map{c=>c._2 match{
        case u:MultiUnionOp=>
        u.children.map(ch=>(ch._2.id,ch._2))
        case _=> Seq(c)
      }
      }.flatten.toMap) 
	else this
  }
}

object MultiUnionOp{
  def apply(childrenOps:Map[String,AlgebraOp])={
    val validChildren=childrenOps.filter(c=>c._2!=null)
    if (validChildren.size>0) new MultiUnionOp(childrenOps)
    else null
  }
}