package es.upm.fi.oeg.morph.stream.query
import collection.JavaConversions._
import org.apache.commons.lang.NotImplementedException
import com.google.common.collect.Maps
import es.upm.fi.dia.oeg.integration.algebra.Window
import es.upm.fi.dia.oeg.common.TimeUnit
import scala.collection.mutable.HashMap
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import es.upm.fi.oeg.morph.stream.algebra.InnerJoinOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.BinaryOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.WindowOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.NullValueXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ValueXpr
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.RelationOp

class SqlQuery(val projectionVars:Map[String,String]) extends SourceQuery{
	protected var innerQuery:String=null
	val	projectList = new HashMap[String,String]();
    val inner= new HashMap[String,Map[String,SqlQuery]]();
	
	override def load(op:AlgebraOp){
		//super.load(op)
		this.innerQuery = build(op)
	}

    override def serializeQuery():String=return innerQuery
    override def supportsPostProc=true
    
    override def getProjection:Map[String,String]=
		return getAugmentedProjectList();
/*
	override def setOriginalQuery(sparqlQuery:String){} 
		// TODO Auto-generated method stub
		
	override def getOriginalQuery():String= null
		// TODO Auto-generated method stub
*/
	
	
    def projExtentAlias(proj:ProjectionOp)=
      proj.getRelation.extentName+" AS "+proj.getRelation.id 
      
    def projConditions(proj:ProjectionOp)=proj.subOp match{
	  case sel:SelectionOp=>sel.expressions.map(e=>e.toString).mkString(" ")
	  case _=>""
	}
	def joinConditions(join:InnerJoinOp)=join.conditions.map(c=>unAliasXpr(join,c)).mkString(" AND ")
	def joinConditions(join:LeftOuterJoinOp)=join.conditions.map(c=>unAliasXpr(join,c)).mkString(" AND ")

	private def conditions(op:AlgebraOp):Seq[String]=op match{
	  case join:InnerJoinOp=>conditions(join)
	  case join:LeftOuterJoinOp=>conditions(join)
	  case _=>List()
	}
	private def conditions(join:BinaryOp):Seq[String]=(join.left,join.right) match{
	  case (lp:ProjectionOp,rp:ProjectionOp)=>List(projConditions(lp),projConditions(rp))
	  case (lp:ProjectionOp,a) =>List(projConditions(lp))++conditions(a)
	  case (a,rp:ProjectionOp) =>List(projConditions(rp))++conditions(a) 
	}

	private def joinXprs(op:AlgebraOp):Seq[String]=op match{
	  case join:InnerJoinOp=>List(joinConditions(join))++joinXprs(join.left)++joinXprs(join.right)
	  case _=>List()
	}
	
	private def get(op:AlgebraOp):Seq[String]=op match{
	  case join:InnerJoinOp=>get(join)
	  case _=>List()
	}
	
	private def getNotNull(p1:String,p2:String)=if (p1==null) p2 else p1
	
	private def get(join:LeftOuterJoinOp):Seq[String]= (join.left,join.right) match {
	  case (lp:ProjectionOp,rp:ProjectionOp)=>
	    List(projExtentAlias(lp)+" LEFT OUTER JOIN "+ projExtentAlias(rp) +" ON "+joinConditions(join))
	  case (lp:ProjectionOp,a) =>List(projExtentAlias(lp))++get(a)
	  case (a,rp:ProjectionOp) =>List(projExtentAlias(rp))++get(a)
	}
	
	private def get(join:InnerJoinOp):Seq[String]=(join.left,join.right) match{
	  case (lp:ProjectionOp,rp:ProjectionOp)=>List(projExtentAlias(lp),projExtentAlias(rp))
	  case (lp:ProjectionOp,a) =>List(projExtentAlias(lp))++get(a)
	  case (a,rp:ProjectionOp) =>List(projExtentAlias(rp))++get(a)
	}

	protected def projVars(op:AlgebraOp):List[String]=op match{
	  case proj:ProjectionOp=>	    
	    proj.getVarMappings.filter(_._2!=null).map(v=>v._2.map(s=>proj.getRelation.id+"."+s)).flatten.toList
	    //proj.getVarMappings().keys.toList
	  case bi:BinaryOp=>projVars(bi.left)++projVars(bi.right)
	}
	
	private def unAliasXpr(join:BinaryOp,xpr:BinaryXpr):String=
	  unAliasXpr(join.left,xpr.left)+xpr.op+unAliasXpr(join.right,xpr.right)

	private def unAliasXpr(op:AlgebraOp,xpr:Xpr):String=(op,xpr) match {
	  case (join:BinaryOp,bi:BinaryXpr)=>unAliasXpr(join,bi)
	  case (join:BinaryOp,varXpr:VarXpr)=>getNotNull(unAliasXpr(join.left,xpr), unAliasXpr(join.right,xpr))	  
	  case (proj:ProjectionOp,varXpr:VarXpr)=> if (proj.getVarMappings.containsKey(varXpr.varName))
	      proj.getRelation.id+"."+proj.getVarMappings.get(varXpr.varName) else null
	  case _=>throw new Exception("Not supported "+op.toString + xpr.toString)
	}
	
  private def build(op:AlgebraOp):String={
	if (op == null)	return "";
	else op match{
	  case root:RootOp=>return build(root.subOp)+";"
	  //case union:OpUnion=>return build(union.left)+" UNION  "+build(union.getRight)			
      case proj:ProjectionOp=>
        return "(SELECT "+ serializeSelect(proj)+" FROM "+build(proj.subOp)+")";
	  case win:WindowOp=>
	    return win.extentName+ serializeWindowSpec(win.windowSpec)+ " "+win.extentName
      case rel:RelationOp=>return rel.extentName
	  case sel:SelectionOp=>
		return build(sel.subOp)+ " WHERE "+serializeExpressions(sel.expressions,null)
	  case join:LeftOuterJoinOp=>
	    var select = "SELECT "+projVars(join)+" FROM "+ get(join).mkString(",") 
		if (!join.conditions.isEmpty)
			select+=" WHERE "+joinXprs(join).mkString(" AND ")+" "+conditions(join).mkString(" AND ")
	    select
	  case join:InnerJoinOp=>
		var select = "SELECT "+projVars(join)+//serializeSelect(opLeft,"",true)+ ", "+serializeSelect(opRight,"2",true) +
				" FROM "+ get(join).mkString(",") //build(opLeft.getSubOp())+","+build(opRight.getSubOp());
		if (!join.conditions.isEmpty())
			select+=" WHERE "+joinXprs(join).mkString(" AND ")+" "+conditions(join).mkString(" AND ");
			
	  return "("+select+")";
		
	  case union:MultiUnionOp=>
		return union.children.values.map(col=>build(col)).mkString(" UNION ")
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }

	protected def serializeSelect(proj:ProjectionOp):String={
		return serializeSelect(proj, "");
	}
	
	protected def serializeExpressions(join:InnerJoinOp):String={
		val varMappings = new HashMap[String,Seq[String]]();
		if (join.left.isInstanceOf[ProjectionOp])
			 varMappings.putAll(join.left.asInstanceOf[ProjectionOp].getVarMappings);
		if (join.right.isInstanceOf[ProjectionOp])
			 varMappings.putAll(join.right.asInstanceOf[ProjectionOp].getVarMappings)
		
		return serializeExpressions(join.conditions, varMappings.toMap);
	}

	private def serializeSelect(proj:ProjectionOp,index:String):String=
	{
		return serializeSelect(proj, index, false);
	}
	protected def serializeSelect(proj:ProjectionOp,index:String,fullExtent:Boolean):String={
		//OpProjection proj = (OpProjection)op;
		var select = "'"+proj.getRelation.extentName+"' AS extentname"+index+", ";
		var pos =0;
		
		proj.expressions.entrySet.foreach{entry=>				
			if (entry.getValue()==NullValueXpr){
				pos+=1
				//continue;
			}
			else {
			var vali:String = null;
			vali = entry.getValue().toString()
			if (fullExtent)
				vali = proj.getRelation.extentName+"."+vali;
			select += vali+ " AS "+entry.getKey();

			if (pos < proj.expressions.size()-1) 
				select += ", ";
			pos+=1;
			}
		}
		if (select.endsWith(", "))//TODO remove this ugly control
			select = select.substring(0,select.length()-2);
		
		return select;
	}
	
	private def unAlias(xpr:Xpr,varMappings:Map[String,String]):String={
		if (varMappings == null)
			return xpr.toString();
		val unalias = 
		if (xpr.isInstanceOf[BinaryXpr])
		{
			val binary = xpr.asInstanceOf[BinaryXpr];
			unAlias(binary.left,varMappings)+" "+binary.op+" "+
					unAlias(binary.right,varMappings);
		}
		else if (xpr.isInstanceOf[VarXpr])
		{
			val vari = xpr.asInstanceOf[VarXpr];
			if (varMappings.containsKey(vari.varName))
			   varMappings(vari.varName);
			else
			   vari.varName
		}
		else if (xpr.isInstanceOf[ValueXpr])
		{
			val vali = xpr.asInstanceOf[ValueXpr]
			vali.value
		}
		else ""
		return unalias;
	}
	
	protected def serializeExpressions(xprs:Collection[Xpr],varMappings:Map[String,Seq[String]]):String={
		var exprs = "";
		var i=0;
		xprs.foreach{xpr=>						
			//exprs+=unAlias(xpr,varMappings) + (if ((i+1) < xprs.size) " AND " else "")
			i+=1;
		}
		return exprs;
	}
	
	
	private def serializeWindowSpec(window:Window):String={
		if (window == null) return "";
		var ser = "[FROM NOW - "+window.getFromOffset()+" "+serializeTimeUnit(window.getFromUnit())+
					" TO NOW - "+window.getToOffset()+" "+serializeTimeUnit(window.getToUnit());
		if (window.getSlideUnit()!=null)
			ser +=	" SLIDE "+window.getSlide()+ " "+serializeTimeUnit(window.getSlideUnit());
		return ser+"]";
	}
	
	private def serializeTimeUnit(tu:TimeUnit):String={
		return tu.toString()+"S";
	}
	

	
	def getAugmentedProjectList():Map[String,String]={		
		/*projectList.values.foreach{att=>			
			val map = inner(extractExtent(att))
			map.values.foreach{q=>
				val at = q.projectList(att.getAlias)
				att.getInnerNames().add(at.getName.toLowerCase)
			}
		}*/
		return projectList.toMap;		
	}
	
	protected def trimExtent(field:String):String={
		val m = field.indexOf('.')		
	    return field.substring(m+1)		
	}
	
	private def extractExtent(field:String):String={
		val m = field.indexOf('.')		
		return field.substring(0,m)		
	}
	
	private def replaceExtent(field:String, extent:String):String={
		val m = field.indexOf('.')		
		return extent+field.substring(m)		
	}
		

}