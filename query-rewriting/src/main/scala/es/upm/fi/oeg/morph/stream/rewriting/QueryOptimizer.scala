package es.upm.fi.oeg.morph.stream.rewriting
import com.weiglewilczek.slf4s.Logging
import es.upm.fi.dia.oeg.integration.algebra.OpInterface
import es.upm.fi.dia.oeg.integration.algebra.OpBinary
import es.upm.fi.dia.oeg.integration.algebra.OpJoin
import es.upm.fi.dia.oeg.integration.algebra.OpProjection
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion
import collection.JavaConversions._
import com.google.common.collect.Sets
import es.upm.fi.dia.oeg.integration.algebra.OpUnary
import es.upm.fi.dia.oeg.integration.algebra.OpSelection
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr
import java.util.TreeMap
import es.upm.fi.dia.oeg.integration.algebra.OpUnion
import org.apache.commons.lang.NotImplementedException

class QueryOptimizer extends Logging{
  def optimizeJoin(join:OpJoin):OpInterface={
    if (join.getLeft == null || join.getRight == null)
	  return null  //join with empty relations is empty
	if (join.getConditions().isEmpty()){
	  logger.debug("Cross product");
	  return join;
	}
	else if (join.getLeft.isInstanceOf[OpProjection] &&
				join.getRight.isInstanceOf[OpProjection]){
	  val left=join.getLeft.asInstanceOf[OpProjection]
	  val right=join.getRight.asInstanceOf[OpProjection]
	  if (join.hasEqualConditions && !left.getSubOp.isInstanceOf[OpSelection] 
	    && !right.getSubOp().isInstanceOf[OpSelection]){
		return join.getLeft.merge(join.getRight, join.getConditions)		
	  }
	}
	else {
	  logger.debug("join merging "+join.getConditions)
	  join.getLeft.display
	  join.getRight.display

	  if (join.getLeft().isInstanceOf[OpJoin]){
		val ljoin = join.getLeft().asInstanceOf[OpJoin]
		if (ljoin.getConditions().isEmpty())// optimize empty cross prod
		{
		  join.setLeft(ljoin.getRight());
		  ljoin.setRight(join);
		  return ljoin;
		}
	  }
					
	  if (join.getRight.isInstanceOf[OpJoin])
		return join
					
	  /* this merges left and right					
	  if (join.hasEqualConditions()){
		val merged = join.getLeft().merge(join.getRight(),join.getConditions());
				
		logger.debug("merged join");
		merged.display();
		return merged;
	  }*/
					
					
	  if (join.getLeft().isInstanceOf[OpMultiUnion])
		//join.getRight() instanceof OpMultiUnion) //push down join in unions
	  {
		  val newunion = new OpMultiUnion(null);					
		  val lunion = join.getLeft().asInstanceOf[OpMultiUnion]
						
		  val rightOps = 
		    if (join.getRight().isInstanceOf[OpMultiUnion]){
			  val runion=join.getRight().asInstanceOf[OpMultiUnion]
			  runion.getChildren().values().toList
		    }
		    else if (join.getRight() .isInstanceOf[OpProjection])
			  List(join.getRight)
		    else List()
						
						
		  var i=0;
		  lunion.getChildren().values().foreach{lo=>
		    rightOps.foreach{ro=>
			  val newjoin = new OpJoin(null,lo.copyOp(),ro.copyOp(), false );
			  newjoin.getConditions().addAll(join.getConditions());
			  newunion.getChildren().put("join"+i, newjoin);
			  i+=1
		   }
		}							
		return newunion;
	  }
	}
    return join
  }
  def staticOptimize(op:OpBinary):OpInterface={
	val l = staticOptimize(op.getLeft)
	val r = staticOptimize(op.getRight)
	op.setLeft(l)
	op.setRight(r)
    op match {
	  case join:OpJoin=>optimizeJoin(join)
	  case union:OpUnion=>throw new NotImplementedException("Optimize union not implemented")
	  case _=>op
	}

  }
  
	def staticOptimize(union:OpMultiUnion):OpInterface={
		logger.debug("optimize: "+union);
		//union.display();
		val toRemove = Sets.newHashSet[String]();
		union.getChildren().entrySet().foreach{entry=>	
			val optimized = staticOptimize(entry.getValue());
			if (optimized!=null)
				union.getChildren().put(entry.getKey(),optimized );
			else
			{
				toRemove.add(entry.getKey());				
			}
		}
		toRemove.foreach{key=>
			union.getChildren().remove(key);
			union.index.keySet.foreach{k=>
				if (union.index.get(k).containsValue(key))
				{
					var rkey:String = null;
					union.index.get(k).keySet.foreach{k2=>
						if (union.index.get(k).get(k2).contains(key))
							rkey = k2;
					}
					union.index.get(k).removeAll(rkey);
				}
				//throw new NotImplementedException("this key "+key);
				//union.index.get(k).inverse().remove(key);
			}
		}
		if (union.getChildren().size()==1)
		{
			logger.debug("simplified");
			//return union.getChildren().values().iterator().next();
		}
		//union.display();
		return union;
		
	}
	
	def staticOptimize(op:OpUnary):OpInterface={
		val o =staticOptimize(op.getSubOp());
		op.setSubOp(o);
		
		if (op.getName().equals("projection"))
		{
			
			val proj = op.asInstanceOf[OpProjection]
			if (proj.getSubOp()==null) //no relation to project
			{
				return null;
			}
			else if (proj.getSubOp().getName().equals("selection"))
			{
				val sel = proj.getSubOp.asInstanceOf[OpSelection]
				logger.debug("remove constant selections");
				sel.getExpressions.foreach{xpr=>
				val cond=xpr.asInstanceOf[BinaryXpr]
				if (cond.getOp().equals("=") && cond.getLeft().isInstanceOf[VarXpr])
				{
					val vari = cond.getLeft.asInstanceOf[VarXpr]  //so ugly code, we can refactor this mess
					if (proj.getExpressions().containsKey(vari.getVarName()) && 
							proj.getExpressions().get(vari.getVarName()).isInstanceOf[OperationXpr] )
					{
						val oper = proj.getExpressions().get(vari.getVarName()).asInstanceOf[OperationXpr];
						if (oper.getOp().equals("constant"))
						{
							val vali = cond.getRight().asInstanceOf[ValueXpr]
							if (vali.getValue().equals((oper.getParam().asInstanceOf[ValueXpr]).getValue()))
							{								
								proj.setSubOp(sel.getSubOp());
								return proj;
							}
							else
							{
								return null;
							}
						}
					}
				}
			
				}
				
			}
			else if (proj.getSubOp().isInstanceOf[OpBinary] ) //push down projection in unions :)
			{
				val union=proj.getSubOp.asInstanceOf[OpBinary]
				
				
				
				val leftProj = proj.copyOp();
				val rightProj = proj.copyOp();
				if (proj.getSubOp().isInstanceOf[OpJoin])
				{
					val join=proj.getSubOp.asInstanceOf[OpJoin]
					join.getConditions.foreach{xpr=>
						xpr.getVars.foreach{vari=>
							if (!proj.getExpressions().containsKey(vari))
							{
								leftProj.addExpression(vari,new VarXpr(vari));
								rightProj.addExpression(vari,new VarXpr(vari));
							}
						}
					}
				}
				leftProj.setSubOp(union.getLeft());
				rightProj.setSubOp(union.getRight());
				val l = staticOptimize(leftProj);
				val r = staticOptimize(rightProj);
				union.setLeft(l);
				union.setRight(r);
				//should simplify here!!!!
				return	union.simplify();
			
				//return union;
			}
			else if (o.isInstanceOf[OpMultiUnion])
			{
				val union =o.asInstanceOf[OpMultiUnion];
				val newChildren = new TreeMap[String, OpInterface]();
				union.getChildren().entrySet.foreach{col=>
					val copy= new OpProjection(proj.getId(),null);
					copy.getExpressions().putAll(proj.getExpressions());
					//Collection<OpInterface> newCol = new ArrayList<OpInterface>();
					//for (OpInterface opi:col.getValue())
					
						copy.setSubOp(col.getValue());
						val newOp = ( staticOptimize(copy));
					
					newChildren.put(col.getKey(),newOp);

				}
				union.setChildren(newChildren);
				return union;
			}
			else
			{
				if (o.isInstanceOf[OpProjection] ) //double projection, eat the inner!!!
				{
					logger.debug("merge double projections "+proj+o);
					val innerProj = o.asInstanceOf[OpProjection]
					//boolean test = true;
					proj.getExpressions().keySet.foreach{key=>
						if (innerProj.getExpressions().containsKey(key))
						{	
							proj.getExpressions().put(key, innerProj.getExpressions().get(key));
							//test=false;
							//break;
						}
						else //inner projection doesn't cover the attributes of the outer one
						{
							proj.getExpressions().put(key, ValueXpr.NullValueXpr);
						}
					}
					//if (test)
						proj.setSubOp(innerProj.getSubOp());
					//else
						//proj.setSubOp(o);
				}
				else if (o.isInstanceOf[OpUnion])
				{
					System.out.println("cosos");
					/*
					OpUnion union = (OpUnion)o;
					OpInterface l = staticOptimize(union.getLeft());
					OpInterface r = staticOptimize(union.getRight());
					union.setRight(r);
					union.setLeft(l);
					return union;*/
				}
				else if (o!=null)  
					proj.setSubOp(o);
				else   // if projection has no relation to project!!!
					return null;
				
				return proj;
			}
		}
		else if (op.isInstanceOf[OpSelection])
		{
			val selection=op.asInstanceOf[OpSelection];
			//BinaryXpr bin = (BinaryXpr) xpr;
			//VarXpr var = (VarXpr) bin.getLeft();
			if (op.getSubOp.isInstanceOf[OpProjection]) //apply the selection to the projection
			{
				val proj = op.getSubOp.asInstanceOf[OpProjection]
				selection.getSelectionVars.foreach{varName=>
					if (proj.getExpressions().containsKey(varName))
					{
						val sel = new OpSelection(selection.getId(),null,selection.getExpressions());
						proj.build(sel);
						return proj;
					}
					else
						return null;
				}
			}
			else if (op.getSubOp.isInstanceOf[OpUnion])
			{
				// push down selection to union children
				val union=op.getSubOp.asInstanceOf[OpUnion]
				val selLeft = new OpSelection(selection.getId(),union.getLeft(),selection.getExpressions());
				//selLeft.getExpressions().addAll(selection.getExpressions());
				val selRight = new OpSelection(selection.getId(),union.getRight(),selection.getExpressions());
				//selRight.getExpressions().addAll(selection.getExpressions());
				val left = staticOptimize(selLeft);
				val right = staticOptimize(selRight);
				return new OpUnion(left, right).simplify();
				
			}
			else if (op.getSubOp().isInstanceOf[OpMultiUnion])
			{
				val union=op.getSubOp.asInstanceOf[OpMultiUnion]
				val newChildren = new TreeMap[String, OpInterface]();
				union.getChildren().entrySet.foreach{col=>
					//Collection<OpInterface> col2 = new ArrayList<OpInterface>();
					//for (OpInterface opi:col.getValue())
					
					val sel = new OpSelection(selection.getId(),col.getValue(),selection.getExpressions());
					//sel.getExpressions().addAll(selection.getExpressions());
					val newChild = staticOptimize(sel);
					//col2.add(newChild);
					
					newChildren.put(col.getKey(), newChild);
				}
				union.setChildren(newChildren);
				return union;
			}
			else if (op.getSubOp.isInstanceOf[OpJoin])
			{
				val join=op.getSubOp.asInstanceOf[OpJoin]
				join.getConditions().addAll(selection.getExpressions());
				return join;
				//throw new NotImplementedException("Not done for joins");
			}
			
		}
		else
		{
			val oi =staticOptimize(op.getSubOp())
			op.setSubOp(oi)
			return op
		}
		return op
	}
	
	def staticOptimize(op:OpInterface):OpInterface={
	  if (op==null) op
	  else
	  op match {
	    case unary:OpUnary=>staticOptimize(unary)
	    case binary:OpBinary=>staticOptimize(binary)
	    case union:OpMultiUnion=>staticOptimize(union)
	    case _ =>op
	  }
	}


}