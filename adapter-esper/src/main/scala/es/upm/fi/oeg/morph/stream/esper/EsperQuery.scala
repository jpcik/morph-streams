package es.upm.fi.oeg.morph.stream.esper
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import scala.collection.mutable.ArrayBuffer
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.GroupOp
import es.upm.fi.oeg.morph.stream.algebra.InnerJoinOp
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import es.upm.fi.oeg.morph.stream.algebra.WindowOp
import es.upm.fi.oeg.morph.stream.algebra.RelationOp
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import es.upm.fi.oeg.morph.stream.algebra.JoinOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.UnassignedVarXpr
import es.upm.fi.oeg.morph.common.TimeUnit
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import scala.reflect.BeanProperty
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.RDFReader
import java.io.FileInputStream
import collection.JavaConversions._
import com.hp.hpl.jena.query.QuerySolution
import es.upm.fi.oeg.morph.voc.RDFFormat

class EsperQuery (projectionVars:Map[String,String]) extends SqlQuery(projectionVars) {
  //val projectionXprs=projectionVars.map(p=>(p._1,VarXpr(p._2))) 
  private val selectXprs=new collection.mutable.HashMap[String,Xpr]
  val allXprs=new collection.mutable.HashMap[String,Xpr]
  private val from=new ArrayBuffer[String]
  private val where=new ArrayBuffer[String]
  val unions=new ArrayBuffer[EsperQuery]
  private var distinct:Boolean=false
  
  def serializeSelect=
    "SELECT "+ (if (distinct) "DISTINCT " else "") + 
      allXprs.map(s=>s._2 +" AS "+s._1).mkString(",")
      
  def generateWhere(op:AlgebraOp,vars:Map[String,Xpr]):Unit=
    generateWhere(op,vars,true)
  
  def generateWhere(op:AlgebraOp,vars:Map[String,Xpr],joinConditions:Boolean):Unit=op match{
    case root:RootOp=>generateWhere(root.subOp,vars)
    case group:GroupOp=>generateWhere(group.subOp,Map())
    case join:InnerJoinOp=>
      val joinXpr = get(join).mkString(",") 
      if (joinConditions && !join.conditions.isEmpty) 			  
		where+=joinXprs(join).mkString(" AND ")		
	  generateWhere(join.left,vars,false)
	  generateWhere(join.right,vars,false)
    case join:LeftOuterJoinOp=>
	  generateWhere(join.left,vars)
	  generateWhere(join.right,vars)
    case sel:SelectionOp=>      
      where++=conditions(sel,vars)
      generateWhere(sel.subOp,vars)
    case proj:ProjectionOp=>generateWhere(proj.subOp,vars)
    case win:WindowOp=>      
    case rel:RelationOp=>where+=getAlias(rel.id)
  }
    
  def generateUnion(op:AlgebraOp):Unit=op match{
    case union:MultiUnionOp=>
      val un=union.children.values.map{opi=>
        val q=new EsperQuery(projectionVars)
        q.load(new RootOp("",opi))
        q
      }
      unions++=un
  }
  
  def generateFrom(op:AlgebraOp):Unit=op match{
    case root:RootOp=>generateFrom(root.subOp)
    case join:LeftOuterJoinOp=>from++=get(join)	  
    case join:JoinOp=>
      val joinXpr = get(join).mkString(",") 
	  generateFrom(join.left)
	  generateFrom(join.right)
    case proj:ProjectionOp=>generateFrom(proj.subOp)
    case sel:SelectionOp=>generateFrom(sel.subOp)
    case group:GroupOp=>generateFrom(group.subOp)
    case win:WindowOp=>      
      from+=win.extentName+window(win)+ " AS "+getAlias(win.id)
    case rel:RelationOp=> from+=extentAlias(rel)
    case union:MultiUnionOp=>
      val un=union.children.values.map{opi=>
        val q=new EsperQuery(projectionVars)
        q.build(new RootOp("",opi))                
      }
      from+=un.mkString(" union ")
  }
  
  def generateSelectVars(op:AlgebraOp):Unit= op match{
    case root:RootOp=>generateSelectVars(root.subOp)
    case proj:ProjectionOp=>
      val isroot=selectXprs.isEmpty
      if (isroot)
        distinct=proj.distinct
      proj.expressions.foreach{e=>
        val xpr=
          if (e._2==UnassignedVarXpr) null
          else e._2
        if (isroot && xpr==null) 
          selectXprs.put(e._1,UnassignedVarXpr)        
        else if (selectXprs.contains(e._1)) xpr match {
          case rep:ReplaceXpr=>
            rep.varNames.foreach(v=>selectXprs.put(e._1+"_"+v,repExpr(VarXpr(v),proj)))
          case x:Xpr=>selectXprs.put(e._1,xpr)  
        } 
        
        xpr match {
          case rep:ReplaceXpr=>
            rep.varNames.foreach(v=>allXprs.put(e._1+"_"+v,repExpr(VarXpr(v),proj)))
          case v:VarXpr=>allXprs.put(e._1,repExpr(v,proj))  
          case null=>
          case _=>  
		}
      }
      generateSelectVars(proj.subOp)
    case group:GroupOp=>
      group.aggs.foreach{agg=>
        allXprs.put(agg._1,repExpr(agg._2,group))
      }
      generateSelectVars(group.subOp)
    case join:JoinOp=>
      generateSelectVars(join.left)
      generateSelectVars(join.right)
    case selec:SelectionOp=>
      generateSelectVars(selec.subOp)
    case _=>
  }
  
  override def getProjection:Map[String,String]={
    projectionVars.map(p=>p._1->null)
  }
  
  //tests for static triples: remove 
  private def fakeQuery(q:String)=
    "SELECT 'pelsius' AS uom,temperature AS value,stationId AS stationId " +
    "FROM method:SparqlGet.getData('"+q+"') AS sp, wunderground.win:time(10 sec) " +
    "WHERE stationId=sp.p1"
  
  override def build(op:AlgebraOp):String={
	if (op == null)	 ""
	else op match{
	  case root:RootOp=>
	    if (root.subOp.isInstanceOf[MultiUnionOp]){
	      generateUnion(root.subOp)
	      unions.map(q=>q.serializeQuery).mkString(" UNION ")
	    }
	    else {
	      generateSelectVars(op)
	      generateFrom(op)
    	  generateWhere(op,allXprs.toMap)
	      val wherestr=if (where.isEmpty) "" else " WHERE "+where.mkString(" AND ")
	      //return fakeQuery(q)
	      serializeSelect+" FROM "+from.mkString(",")+wherestr
	    }
	  
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }
  
  private def interval(time:Double,unit:TimeUnit)=
    if (unit !=null && time>0) time+" "+unit.name
    else "NOW()"
  
  private def window(win:WindowOp)={
    val spec=win.windowSpec
    val to=interval(spec.to,spec.toUnit)
    val from=interval(spec.from,spec.fromUnit)
    ".win:time("+from+")"      
  }
 
}

//** test class for static rdf **//
case class TripleData(@BeanProperty p1:String,@BeanProperty p2:String,@BeanProperty p3:String,
    @BeanProperty p4:String,@BeanProperty p5:String,@BeanProperty p6:String)

object SparqlGet {

  def getvar(sol:QuerySolution,vari:String)=
    if (vari==null) null
    else sol.getLiteral(vari).getString

  def getData(query:String):Array[TripleData] ={
    println("best query: "+query)
  
    val model = ModelFactory.createDefaultModel
    val arp:RDFReader= model.getReader(RDFFormat.TTL)
	arp.read(model,getClass.getClassLoader.getResourceAsStream("sensordata.ttl"),"")    	   
    val qexec = QueryExecutionFactory.create(query, model)
    val res=qexec.execSelect
    val tt:Seq[String]=(1 to 10).map(_=>null)
    val vars:Seq[String]=res.getResultVars.toSeq++tt
    
    res.map{sol=>      
      TripleData(getvar(sol,vars(0)),
          getvar(sol,vars(1)),
          getvar(sol,vars(2)),
          getvar(sol,vars(3)),
          getvar(sol,vars(4)),
          getvar(sol,vars(5)))
          
    }.toArray
    //Array(TripleData("ISANGALL2","rata","topo"),
      //  TripleData("gata","tapa","topo"))
  }
}