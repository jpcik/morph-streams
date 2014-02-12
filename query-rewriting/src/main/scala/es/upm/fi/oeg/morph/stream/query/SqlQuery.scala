package es.upm.fi.oeg.morph.stream.query
import collection.JavaConversions._
import org.apache.commons.lang.NotImplementedException
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
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

class SqlQuery(op:AlgebraOp, val outputMods:Array[Modifiers.OutputModifier]) 
  extends SourceQuery(op){
  private val logger = LoggerFactory.getLogger(this.getClass)

  val selectXprs=new collection.mutable.HashMap[String,Xpr]  
  val unions=new ArrayBuffer[SqlQuery]
  
  private val niceAlias=new collection.mutable.HashMap[String,String]
  private var aliasGen=0
  protected val from=new ArrayBuffer[String]
  protected val where=new ArrayBuffer[String]
  protected var distinct:Boolean=false

  protected lazy val innerQuery:String=build(op)
  lazy val queryExpressions=varXprs(op)
  lazy val rootVarNames=rootVars(op)
  
  protected def getAlias(name:String)=
    if (name==null) ""
    else{
      if (!niceAlias.contains(name)){
        logger.trace("Alias for name: "+name)
        niceAlias.put(name,"rel"+aliasGen)
        aliasGen+=1      
      }
      niceAlias(name)
    }
  
  lazy val isRstream:Boolean=outputMods.exists(_==Modifiers.Rstream)
  lazy val isIstream:Boolean=outputMods.exists(_==Modifiers.Istream)
  lazy val isDstream:Boolean=outputMods.exists(_==Modifiers.Dstream)
  
  override def build(op:AlgebraOp)=""
  override def create={
    val sqf=new SourceQueryFactory
    sqf.create(op)
  }
    
  override def serializeQuery:String=innerQuery
  override def supportsPostProc=true
    
  override def projectionVars=rootVarNames.toArray
  override def construct=null

  protected def condExpr(xpr:Xpr,op:AlgebraOp):Xpr={
    //hack for count *
    val vars=varXprs(op)++Map("*"->VarXpr("*"))
    xpr match {
      case varXpr:VarXpr=>
        if (vars.contains(varXpr.varName)) 
          substXpr(vars(varXpr.varName),getAlias(attRelation(op,varXpr.varName)))
        else if (vars.keys.exists(vr=>vr.startsWith(varXpr.varName))){
          val k=vars.keys.find(vr=>vr.startsWith(varXpr.varName)).get          
          VarXpr(vars(k)+"")
        }         
        else 
          substXpr(varXpr,getAlias(attRelation(op,varXpr.varName)))      
      case bin:BinaryXpr=>BinaryXpr(bin.op,condExpr(bin.left,op),condExpr(bin.right,op))
      case fun:AggXpr=>substXpr(new AggXpr(fun.aggOp,vars(fun.varName).toString),getAlias(attRelation(op,fun.varName)))
      case rep:ReplaceXpr=>substXpr(rep,getAlias(attRelation(op,rep.varNames.head)))
    
      case _=>xpr
    }
  }
  
  private def substXpr(xpr:Xpr,alias:String)={
    val prefix=if (alias==null) "" else alias+"."
    xpr match{
    case rep:ReplaceXpr=>println("this is the concat experience")
      ValueXpr(concatXpr(rep,prefix))
    case v:VarXpr=>VarXpr(prefix+v.varName)
    case agg:AggXpr=>new AggXpr(agg.aggOp,prefix+agg.param)
    case _=>xpr
    }
  }
  
  private def concatXpr(rep:ReplaceXpr,prefix:String)={
    val repVars=rep.varNames.map(v=>(v,prefix+v)).toMap
    val str=rep.template.split('{').map{s=>
      if (s.contains('}')){
        val sp=s.split('}')
        if (sp.size>1) repVars(sp(0))+" || '"+sp(1)+"'"
        else "cast("+repVars(sp(0))+",string)"
      }
      else "'"+s+"'"
    }.mkString(" || ")
    "("+str+")"
  }
    
  def extentAlias(relation:RelationOp)=
    relation.extentName+" AS "+getAlias(relation.id)
    
  def projExtentAlias(proj:ProjectionOp)=
    proj.getRelation.extentName+" AS "+ getAlias(proj.getRelation.id) 
                  
  def projConditions(proj:ProjectionOp)=proj.subOp match{
	case sel:SelectionOp=>sel.expressions.map(e=>condExpr(e,proj).toString).mkString(" ")
	case _=>""	    
  }
	
  def joinConditions(join:InnerJoinOp)=join.conditions.map(c=>unAliasXpr(join,c)).mkString(" AND ")
  def joinConditions(join:LeftOuterJoinOp)=join.conditions.map(c=>unAliasXpr(join,c)).mkString(" AND ")
		
  protected def conditions(op:AlgebraOp):Seq[String]=op match{
	case join:InnerJoinOp=>conditions(join)
	case join:LeftOuterJoinOp=>conditions(join)
	case sel:SelectionOp=>sel.expressions.map(e=>condExpr(e,sel).toString).toSeq
	case proj:ProjectionOp=>conditions(proj.subOp)
	case binary:BinaryOp=>conditions(binary.left)++conditions(binary.right)
	case _=>List()
  }

  protected def joinXprs(op:AlgebraOp):Seq[String]=op match{
	case join:InnerJoinOp=>List(joinConditions(join))++joinXprs(join.left)++joinXprs(join.right)
	case _=>List()
  }

  private def getNotNull(p1:String,p2:String)=if (p1==null) p2 else p1
  protected def varXprs(op:AlgebraOp):Map[String,Xpr]=op match{
    case root:RootOp=>varXprs(root.subOp)
    case proj:ProjectionOp=>proj.expressions++varXprs(proj.subOp)
    case join:InnerJoinOp=>varXprs(join.left)++varXprs(join.right)
    case selec:SelectionOp=>varXprs(selec.subOp)
    case union:MultiUnionOp=>varXprs(union.children.last._2)
    case group:GroupOp=>group.aggs++varXprs(group.subOp)
    case _=>Map[String,Xpr]()
  }
  
  private def rootVars(op:AlgebraOp):Seq[String]=op match{
    case root:RootOp=>rootVars(root.subOp)
    case proj:ProjectionOp=>proj.expressions.keys.toSeq
    case union:MultiUnionOp=>rootVars(union.children.last._2)
  }
  	  
	private def unAliasXpr(join:BinaryOp,xpr:BinaryXpr):String=
	  //condExpr(xpr.left,join.left)+xpr.op+unAliasXpr(join.right,xpr.right) //difference condExpr and unalias?
	  (xpr.op,xpr.left,xpr.right) match{
	  case ("=",l:VarXpr,r:VarXpr) =>
	    unAliasXpr(join.left,xpr.left)+xpr.op+unAliasXpr(join.right,xpr.right)
	  case _  =>	    	    
	    condExpr(xpr.left,join.left)+xpr.op+condExpr(xpr.right,join.right)
	}
	  

	private def unAliasXpr(op:AlgebraOp,xpr:Xpr):String=(op,xpr) match {
	  case (join:BinaryOp,bi:BinaryXpr)=>unAliasXpr(join,bi)
	  case (join:BinaryOp,varXpr:VarXpr)=>getNotNull(unAliasXpr(join.left,xpr), unAliasXpr(join.right,xpr))	  
	  case (proj:ProjectionOp,varXpr:VarXpr)=>
	    if (proj.getVarMappings.containsKey(varXpr.varName))
	      getAlias(proj.getRelation.id)+"."+proj.getVarMappings(varXpr.varName).head else null
	  case _=>throw new Exception("Not supported "+op.toString + xpr.toString)
	}
	
					
  protected def attRelation(op:AlgebraOp,varName:String):String=op match{    
    case root:RootOp=>attRelation(root.subOp,varName)
    case join:JoinOp=>
      val rel=attRelation(join.left,varName)
      if (rel==null) attRelation(join.right,varName)
      else rel
    case proj:ProjectionOp=>
      if (proj.expressions.contains(varName) && proj.getRelation!=null) 
        proj.getRelation.id
      else if (proj.getVarMappings.map(vars=>vars._2).flatten.contains(varName) && proj.getRelation!=null)
        proj.getRelation.id
      else null
    case sel:SelectionOp=>
      if (sel.getRelation!=null) sel.getRelation.id
      else attRelation(sel.subOp,varName)
    case group:GroupOp=>
      if (group.getRelation!=null) group.getRelation.id
      null
        //attRelation(group.subOp,varName)
  }   
  
}

