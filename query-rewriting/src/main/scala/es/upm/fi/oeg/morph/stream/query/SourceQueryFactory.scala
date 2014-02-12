package es.upm.fi.oeg.morph.stream.query

import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.algebra._
import es.upm.fi.oeg.morph.stream.algebra.xpr._
import collection.JavaConversions._

class SourceQueryFactory {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val niceAlias=new collection.mutable.HashMap[String,String]
  private var aliasGen=0
  
  protected def getAlias(name:String)=
    if (name==null) ""
    else {
      if (!niceAlias.contains(name)){
        logger.trace("Alias for name: "+name)
        niceAlias.put(name,"rel"+aliasGen)
        aliasGen+=1      
      }
      niceAlias(name)
    }
  
  def generateUnion(union:MultiUnionOp)={
      val un=union.children.values.map{opi=>
        create(new RootOp("",opi))        
      }
      un
  }
  

  def generateSelectVars(op:AlgebraOp,selectXprs:Map[String,Xpr]):Map[String,Xpr]= op match{
    case root:RootOp=>generateSelectVars(root.subOp,selectXprs)
    case proj:ProjectionOp=>
      val select=proj.expressions.map{e=>        
        e._2 match {
          case rep:ReplaceXpr=>
            rep.varNames.map(v=>e._1+"_"+v->condExpr(VarXpr(v),proj))
          case v:VarXpr=>Map(e._1->condExpr(v,proj))
          case x:Xpr=>Map(e._1->x)  
        }              
      }.flatten.toMap
      generateSelectVars(proj.subOp,selectXprs++select)
    case group:GroupOp=>
      val select=group.aggs.map{agg=>       
        agg._1->condExpr(agg._2,group)
      }.toMap
      generateSelectVars(group.subOp,selectXprs++select)
    case join:JoinOp=>
      generateSelectVars(join.left,selectXprs)++
      generateSelectVars(join.right,selectXprs)
    case selec:SelectionOp=>
      generateSelectVars(selec.subOp,selectXprs)
    case _=> selectXprs
  }

  
  def generateFrom(op:AlgebraOp,streams:Map[String,InsTable]):Map[String,InsTable]=op match{
    case root:RootOp=>generateFrom(root.subOp,streams)
    case join:LeftOuterJoinOp=>	  
      generateFrom(join.left,streams)++generateFrom(join.right,streams)	  
    case join:JoinOp=>
	  generateFrom(join.left,streams)++generateFrom(join.right,streams)
    case proj:ProjectionOp=>generateFrom(proj.subOp,streams)
    case sel:SelectionOp=>generateFrom(sel.subOp,streams)
    case group:GroupOp=>generateFrom(group.subOp,streams)
    case win:WindowOp=>
      val alias=getAlias(win.hashCode.toString)
      streams++Map(alias->InsTable(alias,win.extentName,Some(win.windowSpec)))
    case pat:PatternOp=>
      //streams++ "method:SparqlGet.getData('SELECT * WHERE{"+pat.patternString+"}') AS "+getAlias(pat.id)
      streams
    case rel:RelationOp=>
      logger.trace("from "+rel.id)
      val alias=getAlias(rel.hashCode.toString)
      streams++Map(alias->InsTable(alias,rel.extentName,None))
    case union:MultiUnionOp=>
      streams
      /*val un=union.children.values.map{opi=>
        new EsperQuery(new RootOp("",opi),mods).serializeQuery        
      }
      from+=un.mkString(" union ")*/
  }

  
  def generateWhere(op:AlgebraOp,conds:Seq[Xpr]):Seq[Xpr]=op match{
    case root:RootOp=>generateWhere(root.subOp,conds)
    case group:GroupOp=>generateWhere(group.subOp,conds)
    case join:InnerJoinOp=>
      //val joinXpr = get(join).mkString(",") 
      //if (joinConditions && !join.conditions.isEmpty) 			  
		conds++joinXprs(join)++		
	  generateWhere(join.left,conds)++
	  generateWhere(join.right,conds)
    case join:LeftOuterJoinOp=>
	  generateWhere(join.left,conds)++
	  generateWhere(join.right,conds)
    case sel:SelectionOp=>  
      conditions(sel)++
      generateWhere(sel.subOp,conds)
    case proj:ProjectionOp=>generateWhere(proj.subOp,conds)
    case win:WindowOp=>conds      
    case rel:RelationOp=>conds
  }
   
  
  def create(op:AlgebraOp):InstQuery={
	if (op == null)	 null
	else op match{
	  case root:RootOp=>root.subOp match {
	    case union:MultiUnionOp=>
	      InstQuery(Map(),Seq(),generateUnion(union).toSeq,Map())	      
	    
	    case _=> 
	      val select=generateSelectVars(op,Map())
	      val from=generateFrom(op,Map())
    	  val where=generateWhere(op,Seq())
	      //val wherestr=if (where.isEmpty) "" else " WHERE "+where.mkString(" AND ")
	      //return fakeQuery(q)
	      //val out = if (outputevery==null) "" else outputevery
	      //serializeSelect+" FROM "+from.mkString(",")+wherestr+" "+ out
	      InstQuery(select,where,Seq(),from)
	  }
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }
  
  protected def conditions(op:AlgebraOp):Seq[Xpr]=op match{
	case join:InnerJoinOp=>conditions(join)
	case join:LeftOuterJoinOp=>conditions(join)
	case sel:SelectionOp=>sel.expressions.map(e=>condExpr(e,sel)).toSeq
	case proj:ProjectionOp=>conditions(proj.subOp)
	case binary:BinaryOp=>conditions(binary.left)++conditions(binary.right)
	case _=>List()
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
  
  
  protected def varXprs(op:AlgebraOp):Map[String,Xpr]=op match{
    case root:RootOp=>varXprs(root.subOp)
    case proj:ProjectionOp=>proj.expressions++varXprs(proj.subOp)
    case join:InnerJoinOp=>varXprs(join.left)++varXprs(join.right)
    case selec:SelectionOp=>varXprs(selec.subOp)
    case union:MultiUnionOp=>varXprs(union.children.last._2)
    case group:GroupOp=>group.aggs++varXprs(group.subOp)
    case _=>Map[String,Xpr]()
  }

  
  //def joinConditions(join:InnerJoinOp)=join.conditions.map(c=>unAliasXpr(join,c))
  //def joinConditions(join:LeftOuterJoinOp)=join.conditions.map(c=>unAliasXpr(join,c))
  def joinConditions(join:InnerJoinOp)=join.conditions.map(c=>condExpr(c,join))
  def joinConditions(join:LeftOuterJoinOp)=join.conditions.map(c=>condExpr(c,join))
	
  
	private def unAliasXpr(join:BinaryOp,xpr:BinaryXpr):Xpr=
	  //condExpr(xpr.left,join.left)+xpr.op+unAliasXpr(join.right,xpr.right) //difference condExpr and unalias?
	  (xpr.op,xpr.left,xpr.right) match{
	  case ("=",l:VarXpr,r:VarXpr) =>
	    BinaryXpr(xpr.op,unAliasXpr(join.left,xpr.left),unAliasXpr(join.right,xpr.right))
	  case _  =>	    	    
	    BinaryXpr(xpr.op,condExpr(xpr.left,join.left),condExpr(xpr.right,join.right))
	}
	  
  

	private def unAliasXpr(op:AlgebraOp,xpr:Xpr):Xpr=(op,xpr) match {
	  case (join:BinaryOp,bi:BinaryXpr)=>unAliasXpr(join,bi)
	  case (join:BinaryOp,varXpr:VarXpr)=>getNotNull(unAliasXpr(join.left,xpr), unAliasXpr(join.right,xpr))	  
	  case (proj:ProjectionOp,varXpr:VarXpr)=>
	    if (proj.getVarMappings.containsKey(varXpr.varName))
	      VarXpr(getAlias(proj.getRelation.id)+"."+proj.getVarMappings(varXpr.varName).head) else null
	  case _=>throw new Exception("Not supported "+op.toString + xpr.toString)
	}
	
private def getNotNull(p1:Xpr,p2:Xpr)=if (p1==null) p2 else p1
  
  protected def joinXprs(op:AlgebraOp):Seq[Xpr]=op match{
	case join:InnerJoinOp=>joinConditions(join)++joinXprs(join.left)++joinXprs(join.right)
	case _=>List()
  }

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

case class InsTable(alias:String,name:String,window:Option[WindowSpec])

case class InstQuery(projs:Map[String,Xpr],sels:Seq[Xpr],union:Seq[InstQuery],from:Map[String,InsTable])
