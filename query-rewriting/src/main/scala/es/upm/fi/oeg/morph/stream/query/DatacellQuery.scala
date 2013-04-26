package es.upm.fi.oeg.morph.stream.query
import es.upm.fi.oeg.morph.stream.algebra._
import es.upm.fi.oeg.morph.common.TimeUnit
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.UnassignedVarXpr
import scala.collection.mutable.ArrayBuffer

class DatacellQuery(projectionVars:Map[String,String]) extends SqlQuery(projectionVars,Array()) {
  
  val selectXprs=new collection.mutable.HashMap[String,Xpr]
  val from=new ArrayBuffer[String]
  val where=new ArrayBuffer[String]
  val unions=new ArrayBuffer[String]
  
  def serializeSelect=
    "SELECT "+ selectXprs.map(s=>s._2 +" AS "+s._1).mkString(",")
  
  def generateWhere(op:AlgebraOp):Unit=generateWhere(op,true)
  def generateWhere(op:AlgebraOp,joinConditions:Boolean):Unit=op match{
    case root:RootOp=>generateWhere(root.subOp)
    case group:GroupOp=>generateWhere(group.subOp)
    case join:InnerJoinOp=>
      val joinXpr = get(join).mkString(",") 
      if (joinConditions && !join.conditions.isEmpty) 			  
		where+=joinXprs(join).mkString(" RAND ")		
	  generateWhere(join.left,false)
	  generateWhere(join.right,false)
    case join:LeftOuterJoinOp=>
	  generateWhere(join.left)
	  generateWhere(join.right)
    case sel:SelectionOp=>
      where++=conditions(sel,Map())
      generateWhere(sel.subOp)
    case proj:ProjectionOp=>generateWhere(proj.subOp)
    case win:WindowOp=>      
      where+=getAlias(win.id)+".ts "+ window(win)
    case rel:RelationOp=>      
      where+=getAlias(rel.id)
  }
    
  def generateUnion(op:AlgebraOp):Unit=op match{
    case union:MultiUnionOp=>
      val un=union.children.values.map{opi=>
        val q=new DatacellQuery(projectionVars)
        q.build(new RootOp("",opi))                
      }
      unions++=un
  }
  
  def generateFrom(op:AlgebraOp):Unit=op match{
    case root:RootOp=>generateFrom(root.subOp)
    case join:LeftOuterJoinOp=>
      from++=get(join)
	  //generateFrom(join.left)
	  //generateFrom(join.right)
    case join:JoinOp=>
      val joinXpr = get(join).mkString(",") 
      //if (!join.conditions.isEmpty) 			  
	  //  where+=joinXprs(join).mkString		
	  generateFrom(join.left)
	  generateFrom(join.right)
    case proj:ProjectionOp=>generateFrom(proj.subOp)
    case sel:SelectionOp=>
      //where++=conditions(sel)
      generateFrom(sel.subOp)
    case group:GroupOp=>generateFrom(group.subOp)
    case win:WindowOp=>      
      from+=extentAlias(win)
      //where+=getAlias(win.id)+".ts "+ window(win)
    case rel:RelationOp=>      
      from+=extentAlias(rel)
    case union:MultiUnionOp=>
      val un=union.children.values.map{opi=>
        val q=new DatacellQuery(projectionVars)
        q.build(new RootOp("",opi))                
      }
      from+=un.mkString(" union ")
  }
  
  def generateSelectVars(op:AlgebraOp):Unit= op match{
    case root:RootOp=>
      generateSelectVars(root.subOp)
    case proj:ProjectionOp=>
      val isroot=selectXprs.isEmpty
      proj.expressions.foreach{e=>
        val xpr=if (e._2==UnassignedVarXpr) null
          else e._2 
        if (isroot) selectXprs.put(e._1,xpr)
        else if (selectXprs.contains(e._1)) selectXprs.put(e._1,repExpr(e._2,proj.subOp))
      }
      generateSelectVars(proj.subOp)
    case group:GroupOp=>
      group.aggs.foreach{agg=>
        if (selectXprs.contains(agg._1)) selectXprs.put(agg._1,repExpr(agg._2,group))
      }
      generateSelectVars(group.subOp)
    case join:JoinOp=>
      generateSelectVars(join.left)
      generateSelectVars(join.right)   
    case _=>
  }
  
  override def build(op:AlgebraOp):String={
	if (op == null)	return "";
	else op match{
	  case root:RootOp=>
	    if (root.subOp.isInstanceOf[MultiUnionOp]){
	      generateUnion(root.subOp)
	      return unions.mkString(" UNION ")
	    }
	    else {
	    generateSelectVars(op)
	    generateFrom(op)
	    generateWhere(op)
	    return serializeSelect+" FROM "+from.mkString(",")+" WHERE "+where.mkString(" AND ")//+ build(root.subOp)+";"
	    }
	  //case union:OpUnion=>return build(union.left)+" UNION  "+build(union.getRight)			
      case proj:ProjectionOp=>
        return "(SELECT "+ serializeSelect(proj)+" FROM "+build(proj.subOp)+")";
	  case win:WindowOp=>
	    return win.extentName+ " WHERE ts " + window(win) //+ " "+win.extentName
      case rel:RelationOp=>return rel.extentName
	  case sel:SelectionOp=>
		return build(sel.subOp)+ " AND "+serializeExpressions(sel.expressions.toSeq,null)
	  case join:LeftOuterJoinOp=>
	    var select = "SELECT "+projVars(join)+" FROM "+ get(join).mkString(",") 
		if (!join.conditions.isEmpty)
			select+=" WHERE "+joinXprs(join).mkString(" AND ")+" "+conditions(join).mkString(" AND ")
	    select
	  case join:InnerJoinOp=>
		var select = "SELECT "+projVars(join)+//serializeSelect(opLeft,"",true)+ ", "+serializeSelect(opRight,"2",true) +
				" FROM "+ get(join).mkString(",") //build(opLeft.getSubOp())+","+build(opRight.getSubOp());
		if (!join.conditions.isEmpty)
			select+=" WHERE "+joinXprs(join).mkString(" AND ")+" "+conditions(join).mkString(" AND ");
			
	  return "("+select+")";
		
	  case union:MultiUnionOp=>
		return union.children.values.map(col=>build(col)).mkString(" UNION ")
		
	  case group:GroupOp=>
	    return group.aggs.map(agg=>agg._2 +" AS "+agg._1).mkString(",") +build(group.subOp)
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }
  
  private def interval(time:Double,unit:TimeUnit)=
    if (unit !=null && time>0) "NOW - INTERVAL '"+time+"' "+unit.name
    else "NOW()"
  
  private def window(win:WindowOp)={
    val spec=win.windowSpec
    val to=interval(spec.to,spec.toUnit)
    val from=interval(spec.from,spec.fromUnit)
    "BETWEEN "+to+" AND "+from      
  }
}