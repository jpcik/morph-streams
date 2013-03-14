package es.upm.fi.oeg.morph.stream.query
import collection.JavaConversions._
import org.apache.commons.lang.NotImplementedException
import com.google.common.collect.Maps
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
import es.upm.fi.oeg.morph.stream.algebra.WindowSpec
import es.upm.fi.oeg.morph.common.TimeUnit
import es.upm.fi.oeg.morph.stream.algebra.UnaryOp
import es.upm.fi.oeg.morph.stream.algebra.GroupOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.FunctionXpr
import es.upm.fi.oeg.morph.stream.algebra.JoinOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.AggXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ConstantXpr

class SqlQuery(val projectionVars:Map[String,String]) extends SourceQuery{
  protected var innerQuery:String=null
  val projectList = new HashMap[String,String]();
  val inner= new HashMap[String,Map[String,SqlQuery]]();
  private var aliasGen=0
  private val niceAlias=new collection.mutable.HashMap[String,String]
   var varsX:Map[String,Xpr]=_
  
  
  protected def getAlias(name:String)={
    if (name==null) ""
    else{
      if (!niceAlias.contains(name)){
        println("for name: "+name)
        niceAlias.put(name,"rel"+aliasGen)
        aliasGen+=1      
      }
    niceAlias(name)
    }
  }
  
  override def load(op:AlgebraOp){
	//super.load(op)
    varsX=varXprs(op)
	this.innerQuery = build(op)
  }

    override def serializeQuery():String=return innerQuery
    override def supportsPostProc=true
    
    override def getProjection:Map[String,String]=
		return getAugmentedProjectList();
    override def getConstruct=null
	/*
    private def serExpr(xpr:Xpr,relation:RelationOp):Xpr={
      xpr match {
        case varXpr:VarXpr=>VarXpr(getAlias(relation.id)+"."+varXpr.varName)
        case bin:BinaryXpr=>BinaryXpr(bin.op,serExpr(bin.left,relation),serExpr(bin.right,relation))
        case _=>xpr
      }
    }*/
  protected def condExpr(xpr:Xpr,op:AlgebraOp,vars:Map[String,Xpr]):Xpr={
    xpr match {
      case varXpr:VarXpr=>
        if (vars.contains(varXpr.varName)) VarXpr(""+vars(varXpr.varName))
        else if (vars.keys.exists(vr=>vr.startsWith(varXpr.varName))){
          val k=vars.keys.find(vr=>vr.startsWith(varXpr.varName)).get          
          VarXpr(vars(k)+"")
        } 
        else VarXpr(varXpr.varName)
      case bin:BinaryXpr=>BinaryXpr(bin.op,condExpr(bin.left,op,vars),condExpr(bin.right,op,vars))
      case fun:AggXpr=>new AggXpr(fun.aggOp,getAlias(attRelation(op,fun.varName))+"."+fun.varName)
      case rep:ReplaceXpr=>ValueXpr(concatXpr(rep,op))
      case _=>xpr
    }
  }
  
  private def concatXpr(rep:ReplaceXpr,op:AlgebraOp)={
    val repVars=rep.varNames.map(v=>(v,getAlias(attRelation(op,v))+"."+v)).toMap
    val str=rep.template.split('{').map{s=>
      if (s.contains('}')){
        val sp=s.split('}')
        if (sp.size>1) repVars(sp(0))+" || "+sp(1)
        else "cast("+repVars(sp(0))+",string)"
      }
      else "'"+s+"'"
    }.mkString(" || ")
    "("+str+")"
  }
    
    
  protected def repExpr(xpr:Xpr,op:AlgebraOp):Xpr={
    xpr match {
      case varXpr:VarXpr=>VarXpr(getAlias(attRelation(op,varXpr.varName))+"."+varXpr.varName)
      case bin:BinaryXpr=>BinaryXpr(bin.op,repExpr(bin.left,op),repExpr(bin.right,op))
      case fun:AggXpr=>new AggXpr(fun.aggOp,getAlias(attRelation(op,fun.varName))+"."+fun.varName)
      case _=>xpr
    }
  }
    
    def extentAlias(relation:RelationOp)=
      relation.extentName+" AS "+getAlias(relation.id)
    
    def projExtentAlias(proj:ProjectionOp)=
      proj.getRelation.extentName+" AS "+ getAlias(proj.getRelation.id) 
      
      
    def projConditions(proj:ProjectionOp)=proj.subOp match{
	  case sel:SelectionOp=>sel.expressions.map(e=>repExpr(e,proj).toString).mkString(" ")
	  case _=>""
	}
	def joinConditions(join:InnerJoinOp)=join.conditions.map(c=>unAliasXpr(join,c)).mkString(" AND ")
	def joinConditions(join:LeftOuterJoinOp)=join.conditions.map(c=>unAliasXpr(join,c)).mkString(" AND ")

		
	protected def conditions(op:AlgebraOp,vars:Map[String,Xpr]):Seq[String]=op match{
	  case join:InnerJoinOp=>conditions(join)
	  case join:LeftOuterJoinOp=>conditions(join)
	  case sel:SelectionOp=>sel.expressions.map(e=>condExpr(e,sel,vars).toString).toSeq
	  case _=>List()
	}
	protected def conditions(join:BinaryOp):Seq[String]=(join.left,join.right) match{
	  case (lp:ProjectionOp,rp:ProjectionOp)=>List(projConditions(lp),projConditions(rp))
	  case (lp:ProjectionOp,a) =>List(projConditions(lp))++conditions(a,Map())
	  case (a,rp:ProjectionOp) =>List(projConditions(rp))++conditions(a,Map()) 
	}

	protected def joinXprs(op:AlgebraOp):Seq[String]=op match{
	  case join:InnerJoinOp=>List(joinConditions(join))++joinXprs(join.left)++joinXprs(join.right)
	  case _=>List()
	}
	
	protected def get(op:AlgebraOp):Seq[String]=op match{
	  case join:InnerJoinOp=>get(join)
	  case outerJoin:LeftOuterJoinOp=>get(outerJoin)
	  case _=>List()
	}
	
	private def getNotNull(p1:String,p2:String)=if (p1==null) p2 else p1
	
	protected def get(join:LeftOuterJoinOp):Seq[String]= (join.left,join.right) match {
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

	protected def projVars(op:AlgebraOp):String=op match{
	  case proj:ProjectionOp=>
	    serializeSelect(proj)
	    //proj.getVarMappings.filter(_._2!=null).map(v=>v._2.map(s=>proj.getRelation.extentName+"."+s)).flatten.toList
	    //proj.getVarMappings().keys.toList
	  case bi:BinaryOp=>projVars(bi.left)+projVars(bi.right)
	}
	protected def projVarNames(op:AlgebraOp):Seq[String]=op match{
	  case proj:ProjectionOp=>
	    //serializeSelect(proj)
	    proj.getVarMappings.filter(_._2!=null).map(v=>v._2.map(s=>proj.getRelation.extentName+"."+s)).flatten.toList
	    //proj.getVarMappings().keys.toList
	  case bi:BinaryOp=>projVarNames(bi.left)++projVarNames(bi.right)
	}

  protected def varXprs(op:AlgebraOp):Map[String,Xpr]=op match{
    case root:RootOp=>varXprs(root.subOp)
    case proj:ProjectionOp=>proj.expressions++varXprs(proj.subOp)
    case join:InnerJoinOp=>varXprs(join.left)++varXprs(join.right)
    case selec:SelectionOp=>varXprs(selec.subOp)
    case union:MultiUnionOp=>varXprs(union.children.last._2)
    case _=>Map[String,Xpr]()
  }
  
	
  
	private def unAliasXpr(join:BinaryOp,xpr:BinaryXpr):String=
	  unAliasXpr(join.left,xpr.left)+xpr.op+unAliasXpr(join.right,xpr.right)

	private def unAliasXpr(op:AlgebraOp,xpr:Xpr):String=(op,xpr) match {
	  case (join:BinaryOp,bi:BinaryXpr)=>unAliasXpr(join,bi)
	  case (join:BinaryOp,varXpr:VarXpr)=>getNotNull(unAliasXpr(join.left,xpr), unAliasXpr(join.right,xpr))	  
	  case (proj:ProjectionOp,varXpr:VarXpr)=> 
	    if (proj.getVarMappings.containsKey(varXpr.varName))
	      getAlias(proj.getRelation.id)+"."+proj.getVarMappings(varXpr.varName).head else null
	  case _=>throw new Exception("Not supported "+op.toString + xpr.toString)
	}
	
  protected def build(op:AlgebraOp):String={
	if (op == null)	return "";
	else op match{
	  case root:RootOp=>return build(root.subOp)+";"
	  //case union:OpUnion=>return build(union.left)+" UNION  "+build(union.getRight)			
      case proj:ProjectionOp=>
        return "(SELECT "+ projVars(proj)+" FROM "+build(proj.subOp)+")";
	  case win:WindowOp=>
	    return win.extentName+ serializeWindowSpec(win.windowSpec)+ " "+win.extentName
      case rel:RelationOp=>return rel.extentName
	  case sel:SelectionOp=>
		return build(sel.subOp)+ " WHERE "+serializeExpressions(sel.expressions.toSeq,null)
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
		
	  case group:GroupOp=>
	    return group.aggs.map(agg=>agg._2 +" AS "+agg._1).mkString(",") +build(group.subOp)
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
		return serializeSelect(proj, index, true,false)
	}
	
  protected def serializeSelect(proj:ProjectionOp,index:String,fullExtent:Boolean,extentName:Boolean):String={		
	val relation=proj.getRelation
	  
	val alias=if (relation!=null){
	  getAlias(relation.id.toString)
	}
	else null
	    
	var select =
	  if (relation!=null&&extentName)
		"'"+relation.extentName+"' AS extentname"+index+", ";
	  else ""
	var pos =0;
		
	proj.expressions.entrySet.foreach{entry=>				
	      if (entry.getValue()==NullValueXpr){
			pos+=1
		  }
		  else {
			var vali:String = entry.getValue().toString()
			if (fullExtent)
			  //vali = proj.getRelation.extentName+"."+vali
			  vali = alias+"."+vali
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
	
	protected def serializeExpressions(xprs:Seq[Xpr],varMappings:Map[String,Seq[String]]):String={
		var exprs = "";
		//var i=0;
		xprs.map{xpr=>
		  xpr.toString
			//exprs+=unAlias(xpr,varMappings) + (if ((i+1) < xprs.size) " AND " else "")
			//i+=1;
		}.mkString("AND")
		//return exprs;
	}
	
	
	private def serializeWindowSpec(window:WindowSpec):String={
		if (window == null) return "";
		var ser = "[FROM NOW - "+window.from+" "+serializeTimeUnit(window.fromUnit)
		if  (window.toUnit != null)
			ser += " TO NOW - "+window.to+" "+serializeTimeUnit(window.toUnit);
		if (window.slideUnit!=null)
			ser +=	" SLIDE "+window.slide+ " "+serializeTimeUnit(window.slideUnit);
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
		
  protected def attRelation(op:AlgebraOp,varName:String):String= 
    op match{    
      case root:RootOp=>attRelation(root.subOp,varName)
      case join:JoinOp=>
        val rel=attRelation(join.left,varName)
        if (rel==null) attRelation(join.right,varName)
        else rel
      case proj:ProjectionOp=>
        if (proj.expressions.contains(varName) && proj.getRelation!=null) 
          proj.getRelation.id
        else if (proj.getVarMappings.map(vars=>vars._2).flatten.contains(varName))
          proj.getRelation.id
        else null
      case sel:SelectionOp=>
        if (sel.getRelation!=null) sel.getRelation.id
        else null
      case group:GroupOp=>
        group.getRelation.id
        //attRelation(group.subOp,varName)
    }
    //else null
}