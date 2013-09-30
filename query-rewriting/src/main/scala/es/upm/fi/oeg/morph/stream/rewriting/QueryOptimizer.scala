package es.upm.fi.oeg.morph.stream.rewriting
import collection.JavaConversions._
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
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import scala.compat.Platform
import scala.util.Random

class QueryOptimizer{
  private val logger= LoggerFactory.getLogger(this.getClass)

  def staticOptimize(op:AlgebraOp):AlgebraOp={
	if (op==null) op
	else op match {
	  case unary:UnaryOp=>staticOptimize(unary)
	  case binary:BinaryOp=>staticOptimize(binary)
	  case union:MultiUnionOp=>staticOptimize(union)
	  case _ =>op
	}
  }
  
  def staticOptimize(op:UnaryOp):AlgebraOp= {
	val o = staticOptimize(op.subOp)
	op match {
	  case proj:ProjectionOp=>	
		optimizeProjection(new ProjectionOp(proj.expressions,o,proj.distinct))
	  case selection:SelectionOp=>
	    optimizeSelection(new SelectionOp(selection.id,o,selection.expressions))
	  case root:RootOp=> new RootOp(op.id,o)
	  case group:GroupOp=>
        optimizeGroup(new GroupOp(group.id,group.groupVars,group.aggs,o))
	  case _ => op
	}
  }
  
  def staticOptimize(op:BinaryOp):AlgebraOp={
	val l = staticOptimize(op.left)
	val r = staticOptimize(op.right)	
	op match {
	  case join:InnerJoinOp=>optimizeJoin(new InnerJoinOp(l,r))
	  case join:LeftOuterJoinOp=>optimizeLeftJoin(new LeftOuterJoinOp(l,r))
	  case _=>op
	}
  }
  
  def staticOptimize(union:MultiUnionOp):AlgebraOp={
	logger.trace("optimize union: "+union.toString)		
	if (logger.isTraceEnabled)
	  union.display
	val newChildren=union.children.values.map(op=>staticOptimize(op)).filter(_!=null).zipWithIndex
	val newUnion=new MultiUnionOp(newChildren.map(c=>c._1.id+c._2->c._1).toMap)
	val u=newUnion.simplify
	if (logger.isTraceEnabled){
	  logger.trace("after union optim")
	  u.display
	}
	u
  }
	
  def optimizeLeftJoin(join:LeftOuterJoinOp):AlgebraOp={
    if (join.left == null || join.right == null)
	  null  //join with empty relations is empty
	else if (join.conditions.isEmpty){
	  logger.debug("Cross product join")
	  join
	}
	else {
	  val pushed=pushDownLeftJoin(join)
	  if (pushed==join) join
	  else staticOptimize(pushed) 	    
	}    
  }

  def pushDownJoin(join:InnerJoinOp):AlgebraOp={
    (join.left,join.right) match{
      case (op1:ProjectionOp,op2:ProjectionOp)=>
        logger.trace("push down join "+ join.conditions.mkString("--")+" ")
        logger.trace(join.hasEqualConditions+ join.isCompatible.toString+op1.getRelation)
        if (join.hasEqualConditions && join.isCompatible && 
            op1.getRelation.extentName.equals(op2.getRelation.extentName) &&
            //op1.subOp.isInstanceOf[RelationOp] && op2.subOp.isInstanceOf[RelationOp] &&
            join.isJoinOnPk  ){
          logger.debug("merging projs")
          op1.merge(op2)
        }
        else if (!join.isCompatible) null
        else join        
        
      case (op1:MultiUnionOp,op2:AlgebraOp)=>
        MultiUnionOp(op1.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op2))))
      case (op1:AlgebraOp,op2:MultiUnionOp)=>
        MultiUnionOp(op2.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op1))))

      case (op1:AlgebraOp,op2:JoinOp)=>
        
        val optim=optimizeJoin(new InnerJoinOp(op1,op2.left))
        val optim2=optimizeJoin(new InnerJoinOp(op1,op2.right))
        if (!optim.isInstanceOf[JoinOp]) optimizeJoin(new InnerJoinOp(optim,op2.right))
        else if (!optim2.isInstanceOf[JoinOp]) optimizeJoin(new InnerJoinOp(optim2,op2.left))
        else join
      case (op1:JoinOp,op2:AlgebraOp)=>
        val optim=optimizeJoin(new InnerJoinOp(op2,op1.left))
        val optim2=optimizeJoin(new InnerJoinOp(op2,op1.right))
        if (!optim.isInstanceOf[JoinOp]) optimizeJoin(new InnerJoinOp(optim,op1.right))
        else if (!optim2.isInstanceOf[JoinOp]) optimizeJoin(new InnerJoinOp(optim2,op1.left))
        else join
      case _=>join
    }
  }
  
  def pushDownLeftJoin(join:LeftOuterJoinOp):AlgebraOp={
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
        MultiUnionOp(op1.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op2))))
      case (op1:AlgebraOp,op2:MultiUnionOp)=>
        MultiUnionOp(op2.children.map(c=>c._1->optimizeJoin(new InnerJoinOp(c._2,op1))))
      case _=>join
    }
  }
  
  /*
  def listCombinations(join:JoinOp)=(join.left,join.right) match{
    case (op1:ProjectionOp,op2:ProjectionOp)=>List(join)
    case (op1:MultiUnionOp,op2:AlgebraOp)=>
    
  }*/
  
  def optimizeJoin(join:InnerJoinOp):AlgebraOp={
    //join.display
    if (join.left == null || join.right == null)
	  return null  //join with empty relations is empty
	if (join.conditions.isEmpty && false){
	  logger.debug("Cross product")
	  return join
	}		
	else {
	  //join.display
	  val pushed=pushDownJoin(join)
	  if (pushed==join) return join
	  //else if (pushed.isInstanceOf[JoinOp]) return pushed
	  else return staticOptimize(pushed) 
	}
    //return join
  }
  

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
		new ProjectionOp(proj.expressions,newBin,proj.distinct)
	  case union:MultiUnionOp=>	
	    println("befooooore ")
	    
	    union.display
		val newChildren = 
		  union.children.entrySet.map{col=>
		    println(col.getKey())
		    col.getKey+Random.nextDouble->staticOptimize(new ProjectionOp(proj.expressions, col.getValue,proj.distinct))
		  }.toMap
		  println(newChildren.size)
		val mu=new MultiUnionOp(newChildren)//.simplify
		println("pibibi")
		mu.display
		mu
		//throw new Exception("got here")
	  case group:GroupOp=>
		val newExpr=proj.expressions.map{e=>
		  if (group.aggs.contains(e._1)) 
			(e._1,VarXpr(e._1))
		  else e
		}
		new ProjectionOp(newExpr,o,proj.distinct)
	  case innerProj:ProjectionOp =>//double projection, eat the inner!!!		
		logger.debug("merge double projections "+proj+"\n"+o);
		
		val xprs=
			proj.expressions.keySet.map{key=>
			  if (innerProj.expressions.containsKey(key))
				(key, innerProj.expressions(key))
			  else //inner projection doesn't cover the attributes of the outer one
			    (key, NullValueXpr)
			}.toMap
		new ProjectionOp(proj.expressions++xprs,innerProj.subOp,proj.distinct)				
	  case _=>			
		if (o!=null)  
		  new ProjectionOp(proj.expressions,o,proj.distinct)
		else  null // if projection has no relation to project!!!
		  				
	}		
		
  }
	
  private def replaceXpr(xpr:Xpr,xprs:Map[String,Xpr]):Xpr=xpr match{
    case bin:BinaryXpr=>
      new BinaryXpr(bin.op,replaceXpr(bin.left,xprs),replaceXpr(bin.right,xprs))      
    case varXpr:VarXpr=>
      xprs.getOrElse(varXpr.varName, varXpr)    
    case _=>xpr
  }
  
  def optimizeSelection(selection:SelectionOp):AlgebraOp=selection.subOp match {
    case proj:ProjectionOp=>
        val found=selection.selectionVarNames.map(varName=>
		  proj.expressions.containsKey(varName))
		if (found.forall(_==true)){	
		    val newExprs=selection.expressions.map(x=>replaceXpr(x,proj.expressions))
		    val sel = new SelectionOp(selection.id,proj.subOp,newExprs)
			new ProjectionOp(proj.expressions,sel,proj.distinct)
		}
		else proj
	case union:MultiUnionOp=>
		val newChildren = new TreeMap[String, AlgebraOp]
		union.children.entrySet.foreach{col=>					
		    val sel = new SelectionOp(selection.id,col.getValue(),selection.expressions);
			val newChild = staticOptimize(sel);
			newChildren.put(col.getKey(), newChild);
		}
		new MultiUnionOp(union.children++ newChildren).simplify	
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
	

	
  def optimizeGroup(group:GroupOp)= group.subOp match{
    /*case proj:ProjectionOp=>
      val newAggs=group.aggs.map{agg=>
	    val vari=proj.expressions(agg._2.varName).asInstanceOf[VarXpr].varName
	    (agg._1,new AggXpr(agg._2.aggOp,vari))	    
	  }
	  new GroupOp(group.id,group.groupVars,newAggs,proj.subOp)*/
    case _=>group
  }
	


}