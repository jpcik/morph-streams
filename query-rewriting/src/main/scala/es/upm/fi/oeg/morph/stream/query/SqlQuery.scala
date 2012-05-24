package es.upm.fi.oeg.morph.stream.query
import es.upm.fi.dia.oeg.integration.QueryBase
import es.upm.fi.dia.oeg.integration.SourceQuery
import es.upm.fi.dia.oeg.integration.algebra.OpInterface
import es.upm.fi.dia.oeg.integration.algebra.OpRoot
import es.upm.fi.dia.oeg.integration.algebra.OpBinary
import es.upm.fi.dia.oeg.integration.algebra.OpProjection
import es.upm.fi.dia.oeg.integration.algebra.OpWindow
import es.upm.fi.dia.oeg.integration.algebra.OpRelation
import es.upm.fi.dia.oeg.integration.algebra.OpSelection
import es.upm.fi.dia.oeg.integration.algebra.OpJoin
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion
import collection.JavaConversions._
import org.apache.commons.lang.NotImplementedException
import com.google.common.collect.Maps
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr
import es.upm.fi.dia.oeg.integration.algebra.Window
import es.upm.fi.dia.oeg.common.TimeUnit
import es.upm.fi.dia.oeg.r2o.plan.Attribute
import scala.collection.mutable.HashMap

class SqlQuery extends QueryBase with SourceQuery
{
	protected var innerQuery:String=null
	val	projectList = new HashMap[String,Attribute]();
    val inner= new HashMap[String,Map[String,SqlQuery]]();
	
	override def load(op:OpInterface){
		super.load(op);
		this.innerQuery = build(op);
	}

    override def serializeQuery():String=
		return innerQuery;

    override def getProjectionMap():java.util.Map[String,Attribute]=
		return getAugmentedProjectList();

	override def setOriginalQuery(sparqlQuery:String){} 
		// TODO Auto-generated method stub
		
	override def getOriginalQuery():String= null
		// TODO Auto-generated method stub

    def projExtentAlias(proj:OpProjection)=
      proj.getRelation.getExtentName+" AS "+proj.getRelation.getId 
    def projConditions(proj:OpProjection)=proj.getSubOp match{
	  case sel:OpSelection=>sel.getExpressions().map(e=>e.toString).mkString(" ")
	  case _=>""
	}
	def joinConditions(join:OpJoin)=join.getConditions.map(c=>c.toString).mkString(" AND ")

	private def conditions(op:OpInterface):Seq[String]=op match{
	  case join:OpJoin=>conditions(join)
	  case _=>List()
	}
	private def conditions(join:OpJoin):Seq[String]=(join.getLeft,join.getRight) match{
	  case (lp:OpProjection,rp:OpProjection)=>List(projConditions(lp),projConditions(rp))
	  case (lp:OpProjection,a) =>List(projConditions(lp))++conditions(a)
	  case (a,rp:OpProjection) =>List(projConditions(rp))++conditions(a) 
	}

	private def joinXprs(op:OpInterface):Seq[String]=op match{
	  case join:OpJoin=>List(joinConditions(join))++joinXprs(join.getLeft)++joinXprs(join.getRight)
	  case _=>List()
	}
/*
	private def joinXprs(join:OpBinary):Seq[String]=(join.getLeft,join.getRight) match{
	  case (lp:OpProjection,rp:OpProjection)=>List(joinConditions(lp),joinConditions(rp))
	  case (lp:OpProjection,a) =>List(joinConditions(lp))++joinXprs(a)
	  case (a,rp:OpProjection) =>List(joinConditions(rp))++joinXprs(a) 
	}*/
	
	private def get(op:OpInterface):Seq[String]=op match{
	  case join:OpJoin=>get(join)
	  case _=>List()
	}
	
	private def get(join:OpJoin):Seq[String]=(join.getLeft,join.getRight) match{
	  case (lp:OpProjection,rp:OpProjection)=>List(projExtentAlias(lp),projExtentAlias(rp))
	  case (lp:OpProjection,a) =>List(projExtentAlias(lp))++get(a)
	  case (a,rp:OpProjection) =>List(projExtentAlias(rp))++get(a)
	}
	
	private def build(op:OpInterface):String={
		if (op == null)
			return "";
		if (op.isInstanceOf[OpRoot])
		{
			return build(op.asInstanceOf[OpRoot].getSubOp())+";";
		}
		else if (op.getName().equals("union"))
		{
			val union = op.asInstanceOf[OpBinary];
			return build(union.getLeft()) + " UNION  " + build(union.getRight());			
		}
		else if (op.isInstanceOf[OpProjection])
		{
			val proj = op.asInstanceOf[OpProjection];
			val select = serializeSelect(proj);
			return "(SELECT "+ select+" FROM "+build(proj.getSubOp())+")";
		}
		else if (op.isInstanceOf[OpWindow])
		{
			val win = op.asInstanceOf[OpWindow];
			return win.getExtentName()+ serializeWindowSpec(win.getWindowSpec())+ " "+win.getExtentName();
		}
		else if (op.isInstanceOf[OpRelation])
		{
			val rel = op.asInstanceOf[OpRelation];
			return rel.getExtentName();
		}
		else if (op.isInstanceOf[OpSelection])
		{
			val sel=op.asInstanceOf[OpSelection];
			return build(sel.getSubOp())+ " WHERE "+serializeExpressions(sel.getExpressions(),null); 
		}
		else if (op.isInstanceOf[OpJoin])
		{
			val join=op.asInstanceOf[OpJoin];
			val (opLeft,opRight)=
			if (join.getLeft().isInstanceOf[OpProjection] && 
					join.getRight().isInstanceOf[OpProjection])
			{
				(join.getLeft().asInstanceOf[OpProjection],join.getRight.asInstanceOf[OpProjection])				
			}
			else if (join.getLeft().isInstanceOf[OpMultiUnion] &&
					join.getRight().isInstanceOf[OpMultiUnion] &&
				(join.getLeft().asInstanceOf[OpMultiUnion]).getChildren().size()==1)
			{
				val uLeft = join.getLeft().asInstanceOf[OpMultiUnion]
				val uRight = join.getRight.asInstanceOf[OpMultiUnion]	
				(uLeft.getChildren.values().head.asInstanceOf[OpProjection],
				    uRight.getChildren.values().head.asInstanceOf[OpProjection])
			}else (null,null)
			//else
				//throw new NotImplementedException("Nested join queries not supported in target language");
			var select = "SELECT "+//serializeSelect(opLeft,"",true)+ ", "+serializeSelect(opRight,"2",true) +
				" FROM "+ get(join).mkString(",") //build(opLeft.getSubOp())+","+build(opRight.getSubOp());
			if (!join.getConditions().isEmpty())
			{			
				//System.err.println(unAlias("sada",join));
				select+=" WHERE "+joinXprs(join)+" "+conditions(join).mkString(" AND ");
			}
			return "("+select+")";
		}
		else if (op.isInstanceOf[OpMultiUnion])
		{
			val union=op.asInstanceOf[OpMultiUnion];
			val unionString = union.getChildren().values.map(col=>build(col)).mkString(" UNION ")
			return unionString
		}
		//else if (op.getName().equals("join"))
		else return "";
	}

	protected def serializeSelect(proj:OpProjection):String={
		return serializeSelect(proj, "");
	}
	
	protected def serializeExpressions(join:OpJoin):String={
		val varMappings = new HashMap[String,String]();
		if (join.getLeft().isInstanceOf[OpProjection])
			 varMappings.putAll(join.getLeft.asInstanceOf[OpProjection].getVarMappings());
		if (join.getRight.isInstanceOf[OpProjection])
			 varMappings.putAll(join.getRight.asInstanceOf[OpProjection].getVarMappings)
		
		return serializeExpressions(join.getConditions(), varMappings.toMap);
	}

	private def serializeSelect(proj:OpProjection,index:String):String=
	{
		return serializeSelect(proj, index, false);
	}
	protected def serializeSelect(proj:OpProjection,index:String,fullExtent:Boolean):String={
		//OpProjection proj = (OpProjection)op;
		var select = "'"+proj.getRelation().getExtentName()+"' AS extentname"+index+", ";
		var pos =0;
		
		proj.getExpressions().entrySet.foreach{entry=>				
			if (entry.getValue()==ValueXpr.NullValueXpr){
				pos+=1
				//continue;
			}
			else {
			var vali:String = null;
			vali = entry.getValue().toString()
			if (fullExtent)
				vali = proj.getRelation().getExtentName()+"."+vali;
			select += vali+ " AS "+entry.getKey();

			if (pos < proj.getExpressions().size()-1) 
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
			unAlias(binary.getLeft(),varMappings)+" "+binary.getOp()+" "+
					unAlias(binary.getRight(),varMappings);
		}
		else if (xpr.isInstanceOf[VarXpr])
		{
			val vari = xpr.asInstanceOf[VarXpr];
			if (varMappings.containsKey(vari.getVarName()))
			   varMappings(vari.getVarName());
			else
			   vari.getVarName();
		}
		else if (xpr.isInstanceOf[ValueXpr])
		{
			val vali = xpr.asInstanceOf[ValueXpr]
			vali.getValue();
		}
		else ""
		return unalias;
	}
	
	protected def serializeExpressions(xprs:Collection[Xpr],varMappings:Map[String,String]):String={
		var exprs = "";
		var i=0;
		xprs.foreach{xpr=>						
			exprs+=unAlias(xpr,varMappings) + (if ((i+1) < xprs.size) " AND " else "")
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
	

	
	def getAugmentedProjectList():Map[String,Attribute]={		
		projectList.values.foreach{att=>			
			val map = inner(extractExtent(att.getName))
			map.values.foreach{q=>
				val at = q.projectList(att.getAlias)
				att.getInnerNames().add(at.getName.toLowerCase)
			}
		}
		return projectList.toMap;		
	}	
	
	private def trimExtent(field:String):String={
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
	
	/*
	private String serializeProjection()
	{
		String result = "SELECT "; 
			for (Map.Entry<String,Attribute> proj : projectList.entrySet())
			{
				result = result + trimExtent(proj.getValue().getName())+
				" AS "+proj.getKey()+
				",";
			}		
		//System.out.println("SELECT");
		return result;
	}

	private String buildCondition(Expression planExp,String oldExtent,String newExtent)
	{
		if (planExp.getValue() != null)
		{
			return planExp.getValue();
		}
		if (planExp.getVarName() != null)
		{
			int i = planExp.getAttributeURI().indexOf('.');
			if (oldExtent!=null && planExp.getAttributeURI().substring(0,i).equals(oldExtent))
				return newExtent + planExp.getAttributeURI().substring(i);
			else
				return planExp.getAttributeURI();
		}
		else if (planExp.getFirstMember()!=null)
		{
			String operator = " = ";
			if (planExp.operator==ExpressionOperator.GREATER) operator = " > ";
			else if (planExp.operator==ExpressionOperator.LESSER) operator = " < ";
			else if (planExp.operator==ExpressionOperator.AND) operator = " and ";
			else if (planExp.operator==ExpressionOperator.OR) operator = " or ";
			else if (planExp.operator==ExpressionOperator.ADD) operator = " + ";
			
			String condition1= buildCondition(planExp.getFirstMember(),oldExtent,newExtent);
			String condition2 =buildCondition(planExp.getSecondMember(),oldExtent,newExtent);
			return "("+condition1+") "+operator+ " ("+condition2+")";
		}
		return "";
	}
	to unlock*/
	
	

}