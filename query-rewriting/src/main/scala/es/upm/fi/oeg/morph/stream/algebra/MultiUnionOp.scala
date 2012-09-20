package es.upm.fi.oeg.morph.stream.algebra
import org.apache.commons.lang.NotImplementedException
import com.weiglewilczek.slf4s.Logging
import scala.collection.immutable.TreeMap
import com.google.common.collect.Multimap
import java.util.HashMap
//import org.apache.commons.collections.CollectionUtils
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr

class MultiUnionOp(val id:String,childrenOps:Map[String,AlgebraOp]) 
  extends AlgebraOp with Logging{
	
  val children=childrenOps.filter(_._2!=null)
  override val name="multiunion"
  val index:Map[String,Multimap[String,String]]=new TreeMap[String,Multimap[String,String]]
  
  override def build(op:AlgebraOp):AlgebraOp=op match{
	case multi:MultiUnionOp=>
	  val join = new InnerJoinOp(this, multi)
	  return join
	case proj:ProjectionOp=>
	  val join = new InnerJoinOp(this, proj)
	  return join
	case _=> throw new NotImplementedException("Build for: "+op.toString)
  }

  override def copyOp:AlgebraOp={
	throw new NotImplementedException("we need to do this: ")
  }

  private def tab(level:Int)=(0 until level).map(_=>"\t").mkString  
	
  override def display(level:Int)	{
	logger.warn(tab(level)+name+" "+id+" "+index.keySet.mkString)
	children.values.take(30).foreach(op=>op.display(level+1))
  }

  override def display(){
		display(0)
  }
	
  override def vars=
	children.values.map(child=>child.vars).reduceLeft(_++_)
	
  override def merge(op:AlgebraOp,xprs:Seq[Xpr]):AlgebraOp={
	if (op.isInstanceOf[MultiUnionOp]){
	  val union = op.asInstanceOf[MultiUnionOp];
	  val seti = index.keySet & union.index.keySet
	  logger.debug("merge unions: "+children.keySet+"--"+union.children.keySet)
		
	  val set =  this.children.keySet //CollectionUtils.intersection(this.children.keySet(), union.children.keySet());			
	  val ops=set.map{key=>
		val child =  children(key)
		union.children.keySet.map{key2=>
		  val child2 = union.children(key2)
		  val opnew =child.copyOp.merge(child2.copyOp,xprs)					
		  key+key2->opnew
		}.filter(_._2!=null)
	  }.flatten.toMap	
	  return new MultiUnionOp(id,ops)
					
    }
	/*
		else if (op.isInstanceOf[OpProjection])
		{
			val proj = op.asInstanceOf[OpProjection]
			logger.debug("extent "+proj.getRelation().getExtentName());
			Map<String,AlgebraOp> newChildren = new TreeMap<String,AlgebraOp>();
			for (Entry<String, AlgebraOp> child:children.entrySet())
			{
				AlgebraOp ch = child.getValue();
				AlgebraOp newchild = ch.merge(proj,xprs);
				if (newchild!=null)
					newChildren.put(child.getKey(), newchild);
			}
			this.setChildren(newChildren);
			return this;
		}*/
	else
		throw new NotImplementedException("Merge implementation missing: "+op.toString());
		
  }
}