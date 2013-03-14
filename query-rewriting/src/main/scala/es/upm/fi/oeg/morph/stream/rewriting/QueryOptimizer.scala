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
import es.upm.fi.oeg.morph.stream.algebra.GroupOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.AggXpr

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
            op1.subOp.isInstanceOf[RelationOp] && op2.subOp.isInstanceOf[RelationOp] &&
            join.isJoinOnPk  ){
          logger.debug("merging projs")
          op1.merge(op2)
        }
        else if (!join.isCompatible) null
        else join        
        
      case (op1:MultiUnionOp,op2:AlgebraOp)=>
        MultiUnionOp(op1.id,op1.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op2))))
      case (op1:AlgebraOp,op2:MultiUnionOp)=>
        MultiUnionOp(op2.id,op2.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op1))))
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
	logger.debug("optimize: "+union.toString)			
	val newChildren=union.children.values.map(op=>staticOptimize(op)).filter(_!=null).zipWithIndex
	val newUnion=new MultiUnionOp(union.id,newChildren.map(c=>c._1.id+c._2->c._1).toMap)
	newUnion.simplify
  }
	
  /*  
  private def simplify(union:MultiUnionOp)={
	if (union.children.isEmpty) null
	if (union.children.size==1)
	  union.children.head._2
	else union
  }*/
	
  def optimizeProjection(proj:ProjectionOp):AlgebraOp={
    val o =proj.subOp
    if (proj.subOp==null) //no relation to project
	  null
	else o match {
	  case bin:BinaryOp=>
		val addVars=if (proj.subOp.isInstanceOf[InnerJoinOp]){
		  val join=proj.subOp.asInstanceOf[InnerJoinOp]
		  join.conditions.map{xpr=>xpr.varNames.filter(vr=> !proj.expressions.contains(vr))
					}.flatten.map(name=>name->VarXpr(name))
		  }
		  else List()
				
		val l = staticOptimize(bin.left);
		val r = staticOptimize(bin.right);				
		val newBin=proj.subOp match {
		  case inner:InnerJoinOp=> new InnerJoinOp(l,r)
		  case outer:LeftOuterJoinOp=> new LeftOuterJoinOp(l,r)
		  case _=>throw new Exception("unsupported optimization")
		}
		//should simplify here!!!!
		new ProjectionOp(proj.id,proj.expressions,newBin,proj.distinct)
	  case union:MultiUnionOp=>	
		val newChildren = 
		  union.children.entrySet.map{col=>
				  val copy= new ProjectionOp(proj.id,proj.expressions, col.getValue,proj.distinct)
				  val newOp=staticOptimize(copy)
				  col.getKey->newOp
				}.toMap
		new MultiUnionOp(union.id,newChildren)
	  case group:GroupOp=>
		val newExpr=proj.expressions.map{e=>
		  if (group.aggs.contains(e._1)) 
			(e._1,VarXpr(e._1))
		  else e
		}
		new ProjectionOp(proj.id,newExpr,o,proj.distinct)
	  case _ =>
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
			new ProjectionOp(proj.id,proj.expressions++xprs,innerProj.subOp,proj.distinct)				
		}		
		else if (o!=null)  
		  new ProjectionOp(proj.id,proj.expressions,o,proj.distinct)
		else   // if projection has no relation to project!!!
		  null;
				//return proj;
	}		
		
  }
	
	def optimizeSelection(selection:SelectionOp):AlgebraOp={
	  selection.subOp match {
	    case proj:ProjectionOp=>
	      val found=selection.selectionVarNames.map(varName=>
			proj.expressions.containsKey(varName))
		  if (found.forall(_==true)){	
			  val sel = new SelectionOp(selection.id,proj.subOp,selection.expressions)
			  new ProjectionOp(proj.id,proj.expressions,sel,proj.distinct)
		  }
		  else proj
	    case union:MultiUnionOp=>
		  val newChildren = new TreeMap[String, AlgebraOp]
		  union.children.entrySet.foreach{col=>					
		    val sel = new SelectionOp(selection.id,col.getValue(),selection.expressions);
			val newChild = staticOptimize(sel);
			newChildren.put(col.getKey(), newChild);
		  }
		  new MultiUnionOp(union.id,union.children++ newChildren)			
	    case join:InnerJoinOp=>
		  logger.debug("selection inside join "+join.toString)
		  //val selxprs=selection.expressions.map(_.asInstanceOf[BinaryXpr]).toList
		  val selVars=selection.expressions.map(x=>x.varNames).flatten
		  if (join.left.vars.containsAll(selVars) && join.right.vars.containsAll(selVars)){		  
		    val l=new SelectionOp(selection.id,join.left,selection.expressions)
		    val r=new SelectionOp(selection.id,join.right,selection.expressions)
		    new InnerJoinOp(staticOptimize(l),staticOptimize(r))
		  }
		  else selection
	    case _=>selection
	   }
	}
	
  def staticOptimize(op:UnaryOp):AlgebraOp= {
	val o = staticOptimize(op.subOp)
	op match {
	  case proj:ProjectionOp=>	
		optimizeProjection(new ProjectionOp(proj.id,proj.expressions,o,proj.distinct))
	  case selection:SelectionOp=>
	    optimizeSelection(new SelectionOp(selection.id,o,selection.expressions))
	  case root:RootOp=> new RootOp(op.id,o)
	  case group:GroupOp=>
        optimizeGroup(new GroupOp(group.id,group.groupVars,group.aggs,o))
	  case _ => op
	}
  }
	
  def optimizeGroup(group:GroupOp)={
	val proj=group.subOp.asInstanceOf[ProjectionOp]
	val newAggs=group.aggs.map{agg=>
	  val vari=proj.expressions(agg._2.varName).asInstanceOf[VarXpr].varName
	  (agg._1,new AggXpr(agg._2.aggOp,vari))	    
	}
	new GroupOp(group.id,group.groupVars,newAggs,proj.subOp)
  }
	
  def staticOptimize(op:AlgebraOp):AlgebraOp={
	if (op==null) op
	else op match {
	  case unary:UnaryOp=>staticOptimize(unary)
	  case binary:BinaryOp=>staticOptimize(binary)
	  case union:MultiUnionOp=>staticOptimize(union)
	  case _ =>op
	}
  }

}