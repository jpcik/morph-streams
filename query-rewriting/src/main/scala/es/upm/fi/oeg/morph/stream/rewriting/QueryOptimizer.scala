package es.upm.fi.oeg.morph.stream.rewriting
import com.weiglewilczek.slf4s.Logging
import collection.JavaConversions._
import com.google.common.collect.Sets
import java.util.TreeMap
import org.apache.commons.lang.NotImplementedException
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.algebra.InnerJoinOp
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import es.upm.fi.oeg.morph.stream.algebra.BinaryOp
import es.upm.fi.oeg.morph.stream.algebra.UnaryOp
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.OperationXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ValueXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.NullValueXpr
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import es.upm.fi.oeg.morph.stream.algebra.JoinOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.XprUtils
import es.upm.fi.oeg.morph.stream.algebra.RelationOp

class QueryOptimizer extends Logging{
  def optimizeLeftJoin(join:LeftOuterJoinOp):AlgebraOp={
    if (join.left == null || join.right == null)
	  return null  //join with empty relations is empty
	if (join.conditions.isEmpty){
	  logger.debug("Cross product");
	  return join
	}
    return join
  }

  def pushDownJoin(join:InnerJoinOp):AlgebraOp={
    (join.left,join.right) match{
      case (op1:ProjectionOp,op2:ProjectionOp)=>
        logger.debug("push down join "+ join.conditions.mkString("--")+" ")
        logger.debug(join.hasEqualConditions+ join.isCompatible.toString+op1.getRelation)
        if (join.hasEqualConditions && join.isCompatible && 
            op1.getRelation.extentName.equals(op2.getRelation.extentName) &&
            op1.subOp.isInstanceOf[RelationOp] && op2.subOp.isInstanceOf[RelationOp]
            ){
          logger.debug("merging projs")
          op1.merge(op2)
        }
        else if (!join.isCompatible) null
        else join        
      //case (m1:MultiUnionOp,m2:MultiUnionOp)=>
        
      case (op1:MultiUnionOp,op2:AlgebraOp)=>new MultiUnionOp(op1.id,
          op1.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op2))))
      case (op1:AlgebraOp,op2:MultiUnionOp)=>new MultiUnionOp(op2.id,
          op2.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op1))))
      case _=>join
    }
  }
  
  def listCombinations(join:JoinOp)=(join.left,join.right) match{
    case (op1:ProjectionOp,op2:ProjectionOp)=>List(join)
    case (op1:MultiUnionOp,op2:AlgebraOp)=>
    
  }
  
  def optimizeJoin(join:InnerJoinOp):AlgebraOp={
    
    if (join.left == null || join.right == null)
	  return null  //join with empty relations is empty
	if (join.conditions.isEmpty){
	  logger.debug("Cross product")
	  return join
	}		
	else if (false) {
	  logger.debug("join merging "+join.conditions)
	  join.left.display
	  join.right.display

	  if (join.left.isInstanceOf[InnerJoinOp]){
		val ljoin = join.left.asInstanceOf[InnerJoinOp]
		if (ljoin.conditions.isEmpty)// optimize empty cross prod
		{
		  return new InnerJoinOp(ljoin.left,new InnerJoinOp(ljoin.right,join.right))
		}
	  }
					
	  if (join.right.isInstanceOf[InnerJoinOp])
		return join
	  if (join.left.isInstanceOf[MultiUnionOp])
		//join.getRight() instanceof OpMultiUnion) //push down join in unions
	  {
		  val lunion = join.left.asInstanceOf[MultiUnionOp]
						
		  val rightOps = 
		    if (join.right.isInstanceOf[MultiUnionOp]){
			  val runion=join.right.asInstanceOf[MultiUnionOp]
			  runion.children.values.toList
		    }
		    else if (join.right.isInstanceOf[ProjectionOp])
			  List(join.right)
		    else List()
						
						
		  var i=0;
		  val children=lunion.children.values.map{lo=>
		    rightOps.map{ro=>
			  val newjoin = new InnerJoinOp(lo.copyOp,ro.copyOp)//, false ) why is the generate condition false?
			  //newjoin.conditions.addAll(join.conditions);
			  val d="join"+i-> newjoin
			  i+=1
			  d
		     }
		   }.flatten.toMap
     	val newunion = new MultiUnionOp(null,children)				
 
		return newunion;
	  }
	}
	else {
	  val pushed=pushDownJoin(join)
	  if (pushed==join) return join
	  else return staticOptimize(pushed) 
	    //return staticOptimize(pushDownJoin(join))
	}
    return join
  }
  
  def staticOptimize(op:BinaryOp):AlgebraOp={
	val l = staticOptimize(op.left)
	val r = staticOptimize(op.right)	
	op match {
	  case join:InnerJoinOp=>optimizeJoin(new InnerJoinOp(l,r))
	  case join:LeftOuterJoinOp=>optimizeLeftJoin(new LeftOuterJoinOp(l,r))
	  //case union:UnionOp=>throw new NotImplementedException("Optimize union not implemented")
	  case _=>op
	}
  }
  
	def staticOptimize(union:MultiUnionOp):AlgebraOp={
		logger.debug("optimize: "+union)		
		
		val newChildren=union.children.values.map(op=>staticOptimize(op)).filter(_!=null)
		/*
		val grouped=newChildren.filter(_.isInstanceOf[UnaryOp]).groupBy(_.asInstanceOf[UnaryOp].getRelation.extentName)
		val merged=grouped.map{g=>
		  val xprs= g._2.map{op=>op match{
		    case proj:ProjectionOp=>proj.expressions
		    case _=>null
		  }}.flatten.toMap
		  new ProjectionOp(null,xprs,g._2.head.asInstanceOf[UnaryOp].getRelation)
		}*/
		.zipWithIndex
		val newUnion=new MultiUnionOp(union.id,newChildren.map(c=>c._1.id+c._2->c._1).toMap)
		simplify(newUnion)
	}
	
	private def simplify(union:MultiUnionOp)={
	  if (union.children.size==1)
	    union.children.head._2
	  else union
	}
	
	def optimizeProjection(proj:ProjectionOp):AlgebraOp={
	  	val o =proj.subOp

	  	if (proj.subOp==null) //no relation to project
			{
				return null;
			}
			else if (proj.subOp.name.equals("selectionxxx"))
			{
				val sel = proj.subOp.asInstanceOf[SelectionOp]
				logger.debug("remove constant selections");
				sel.expressions.foreach{xpr=>
				val cond=xpr.asInstanceOf[BinaryXpr]
				if (cond.op.equals("=") && cond.left.isInstanceOf[VarXpr])
				{
					val vari = cond.left.asInstanceOf[VarXpr]  //so ugly code, we can refactor this mess
					if (proj.expressions.containsKey(vari.varName) && 
							proj.expressions.get(vari.varName).isInstanceOf[OperationXpr] )
					{
						val oper = proj.expressions.get(vari.varName).asInstanceOf[OperationXpr];
						if (oper.op.equals("constant"))
						{
							val vali = cond.right.asInstanceOf[ValueXpr]
							if (vali.value.equals((oper.param.asInstanceOf[ValueXpr]).value))
							{								
								//proj.setSubOp(sel.getSubOp());
								return new ProjectionOp(proj.id,proj.expressions,sel.subOp)
							}
							else
							{
								return null;
							}
						}
					}
				}
			
				}
				proj
				
			}
			else if (proj.subOp.isInstanceOf[BinaryOp] ) //push down projection in binary :)
			{
				val bin=proj.subOp.asInstanceOf[BinaryOp]								
				
				val addVars=if (proj.subOp.isInstanceOf[InnerJoinOp]){
					val join=proj.subOp.asInstanceOf[InnerJoinOp]
					join.conditions.map{xpr=>
						xpr.varNames.filter(vr=> !proj.expressions.contains(vr))
					}.flatten.map(name=>name->VarXpr(name))
				}
				else List()
				//val leftProj = new ProjectionOp(proj.id,proj.expressions++addVars.toMap,bin.left)
				//val rightProj = new ProjectionOp(proj.id,proj.expressions++addVars.toMap,bin.right)
				val l = staticOptimize(bin.left);
				val r = staticOptimize(bin.right);				
				val newBin=proj.subOp match {
				  case inner:InnerJoinOp=> new InnerJoinOp(l,r)
				  case outer:LeftOuterJoinOp=> new LeftOuterJoinOp(l,r)
				  case _=>throw new Exception("unsupported optimization")
				}
				//should simplify here!!!!
				return	new ProjectionOp(proj.id,proj.expressions,newBin)
				//return newBin
			
				//return union;
			}
			else if (o.isInstanceOf[MultiUnionOp])
			{
				val union =o.asInstanceOf[MultiUnionOp]
				val newChildren = 
				union.children.entrySet.map{col=>
				  val copy= new ProjectionOp(proj.id,proj.expressions, col.getValue)
				  val newOp=staticOptimize(copy)
				  col.getKey->newOp
				}.toMap
				return new MultiUnionOp(union.id,newChildren)
			}
			else
			{
				if (o.isInstanceOf[ProjectionOp] ) //double projection, eat the inner!!!
				{
					logger.debug("merge double projections "+proj+"\n"+o);
					val innerProj = o.asInstanceOf[ProjectionOp]
					val xprs=
					proj.expressions.keySet.map{key=>
						if (innerProj.expressions.containsKey(key))
							(key, innerProj.expressions(key))
						else //inner projection doesn't cover the attributes of the outer one
							(key, NullValueXpr)
					}.toMap
					return new ProjectionOp(proj.id,proj.expressions++xprs,innerProj.subOp)
						//proj.setSubOp(innerProj.getSubOp());
				}
				//else if (o.isInstanceOf[OpUnion])
				//{
					//System.out.println("cosos");
					/*
					OpUnion union = (OpUnion)o;
					AlgebraOp l = staticOptimize(union.getLeft());
					AlgebraOp r = staticOptimize(union.getRight());
					union.setRight(r);
					union.setLeft(l);
					return union;*/
				//}
				else if (o!=null)  
					return new ProjectionOp(proj.id,proj.expressions,o)
				else   // if projection has no relation to project!!!
					return null;
				return proj;
			}		
		
	}
	
	def optimizeSelection(selection:SelectionOp):AlgebraOp={
	  	if (selection.subOp.isInstanceOf[ProjectionOp]) //apply the selection to the projection
			{
				val proj = selection.subOp.asInstanceOf[ProjectionOp]
				selection.selectionVarNames.foreach{varName=>
					if (proj.expressions.containsKey(varName))
					{
						val sel = new SelectionOp(selection.id,proj.subOp,selection.expressions)
						//proj.build(sel);
						return new ProjectionOp(proj.id,proj.expressions,sel)
					}
					else
						return proj;
				}
			}/*
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
				
			}*/
			else if (selection.subOp.isInstanceOf[MultiUnionOp])
			{
				val union=selection.subOp.asInstanceOf[MultiUnionOp]
				val newChildren = new TreeMap[String, AlgebraOp]();
				union.children.entrySet.foreach{col=>
					//Collection<AlgebraOp> col2 = new ArrayList<AlgebraOp>();
					//for (AlgebraOp opi:col.getValue())
					
					val sel = new SelectionOp(selection.id,col.getValue(),selection.expressions);
					//sel.getExpressions().addAll(selection.getExpressions());
					val newChild = staticOptimize(sel);
					//col2.add(newChild);
					
					newChildren.put(col.getKey(), newChild);
				}
				union.children.putAll(newChildren);
				return union;
			}
			else if (selection.subOp.isInstanceOf[InnerJoinOp])	{
				val join=selection.subOp.asInstanceOf[InnerJoinOp]
			  logger.debug("selection inside join "+join.toString)
				val selxprs=selection.expressions.map(_.asInstanceOf[BinaryXpr]).toList
				//join.conditions.addAll(selxprs)
				val l=new SelectionOp(selection.id,join.left,selection.expressions)
				val r=new SelectionOp(selection.id,join.right,selection.expressions)
				return new InnerJoinOp(staticOptimize(l),staticOptimize(r))
				//return join;
				//throw new NotImplementedException("Not done for joins");
			}
		return selection
	}
	
	def staticOptimize(op:UnaryOp):AlgebraOp={
		val o =staticOptimize(op.subOp);
		//op.setSubOp(o);
		
		if (op.name.equals("projection"))
		{
			
			val proj = op.asInstanceOf[ProjectionOp]
		  return optimizeProjection(new ProjectionOp(proj.id,proj.expressions,o))
		}
		else if (op.isInstanceOf[SelectionOp])
		{
			val selection=op.asInstanceOf[SelectionOp];
			return optimizeSelection(new SelectionOp(selection.id,o,selection.expressions))
			//BinaryXpr bin = (BinaryXpr) xpr;
			//VarXpr var = (VarXpr) bin.getLeft();
			
		}
		else if (op.isInstanceOf[RootOp])
		{
			//val oi =staticOptimize(op.subOp)
			//op.setSubOp(oi)
			return new RootOp(op.id,o)
		}
		return op
	}
	
	def staticOptimize(op:AlgebraOp):AlgebraOp={
	  if (op==null) op
	  else
	  op match {
	    case unary:UnaryOp=>staticOptimize(unary)
	    case binary:BinaryOp=>staticOptimize(binary)
	    case union:MultiUnionOp=>staticOptimize(union)
	    case _ =>op
	  }
	}


}