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
import scala.beans.BeanProperty
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.RDFReader
import java.io.FileInputStream
import collection.JavaConversions._
import com.hp.hpl.jena.query.QuerySolution
import es.upm.fi.oeg.morph.voc.RDFFormat
import es.upm.fi.oeg.morph.stream.query.Modifiers
import es.upm.fi.oeg.morph.stream.algebra.PatternOp
import org.slf4j.LoggerFactory

class EsperQuery (op:AlgebraOp,mods:Array[Modifiers.OutputModifier]) 
  extends SqlQuery(op,mods) {
  
  lazy val unionQueries:Seq[EsperQuery]=unions.asInstanceOf[Seq[EsperQuery]]
  var outputevery=" "
  
  def serializeSelect=
    "SELECT "+ 
    (if (mods.contains(Modifiers.Dstream)) "RSTREAM " else "") +
    (if (mods.contains(Modifiers.Istream)) "ISTREAM " else "") +
    (if (mods.contains(Modifiers.Rstream)) "IRSTREAM " else "") +
    (if (distinct) "DISTINCT " else "") +
      selectXprs.map(s=>s._2 +" AS "+s._1).mkString(",")
      
  def generateWhere(op:AlgebraOp):Unit=generateWhere(op,true)
  
  def generateWhere(op:AlgebraOp,joinConditions:Boolean):Unit=op match{
    case root:RootOp=>generateWhere(root.subOp)
    case group:GroupOp=>generateWhere(group.subOp)
    case join:InnerJoinOp=>
      //val joinXpr = get(join).mkString(",") 
      if (joinConditions && !join.conditions.isEmpty) 			  
		where+=joinXprs(join).mkString(" AND ")		
	  generateWhere(join.left,false)
	  generateWhere(join.right,false)
    case join:LeftOuterJoinOp=>
	  generateWhere(join.left)
	  generateWhere(join.right)
    case sel:SelectionOp=>  
      where++=conditions(sel)
      generateWhere(sel.subOp)
    case proj:ProjectionOp=>generateWhere(proj.subOp)
    case win:WindowOp=>      
    case rel:RelationOp=>
  }
    
  def generateUnion(op:AlgebraOp):Unit=op match{
    case union:MultiUnionOp=>
      val un=union.children.values.map{opi=>
        new EsperQuery(new RootOp("",opi),mods)        
      }
      unions++=un
  }
  
  def generateFrom(op:AlgebraOp):Unit=op match{
    case root:RootOp=>generateFrom(root.subOp)
    case join:LeftOuterJoinOp=>//from++=get(join)	  
    case join:JoinOp=>
      //val joinXpr = get(join).mkString(",") 
	  generateFrom(join.left)
	  generateFrom(join.right)
    case proj:ProjectionOp=>generateFrom(proj.subOp)
    case sel:SelectionOp=>generateFrom(sel.subOp)
    case group:GroupOp=>generateFrom(group.subOp)
    case win:WindowOp=>      
      from+=win.extentName+window(win)+ " AS "+getAlias(win.id)
    case pat:PatternOp=>
      from+="method:SparqlGet.getData('SELECT * WHERE{"+pat.patternString+"}') AS "+getAlias(pat.id)      
    case rel:RelationOp=> from+=extentAlias(rel)
    case union:MultiUnionOp=>
      val un=union.children.values.map{opi=>
        new EsperQuery(new RootOp("",opi),mods).serializeQuery        
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
        else if (selectXprs.contains(e._1) || (isroot && xpr!=null) ) xpr match {
          case rep:ReplaceXpr=>
            rep.varNames.foreach(v=>selectXprs.put(e._1+"_"+v,condExpr(VarXpr(v),proj)))
          case v:VarXpr=>selectXprs.put(e._1,condExpr(v,proj))
          case x:Xpr=>selectXprs.put(e._1,xpr)  
        }              
      }
      generateSelectVars(proj.subOp)
    case group:GroupOp=>
      group.aggs.foreach{agg=>       
        selectXprs.put(agg._1,condExpr(agg._2,group))
      }
      generateSelectVars(group.subOp)
    case join:JoinOp=>
      generateSelectVars(join.left)
      generateSelectVars(join.right)
    case selec:SelectionOp=>
      generateSelectVars(selec.subOp)
    case _=>
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
	      //generateAllVars(op)
	      generateSelectVars(op)
	      selectXprs.filter(_._2==UnassignedVarXpr).foreach(t=>selectXprs.remove(t._1))
	      generateFrom(op)
    	  generateWhere(op)
	      val wherestr=if (where.isEmpty) "" else " WHERE "+where.mkString(" AND ")
	      //return fakeQuery(q)
	      val out = if (outputevery==null) "" else outputevery
	      serializeSelect+" FROM "+from.mkString(",")+wherestr+" "+ out
	    }
	  
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }
  
  private def interval(time:Double,unit:TimeUnit)=
    if (unit !=null && time>0) time+" "+unit.name
    else if (time==0) "1 "+TimeUnit.MILLISECOND.name //unit.name
    else "NOW()"
  
  private def window(win:WindowOp)={
    val spec=win.windowSpec
    val to=interval(spec.to,spec.toUnit)
    val from=interval(spec.from,spec.fromUnit)
    if (spec.slideUnit!=null){
      val percent=spec.slide*2d/100
      val every=if (spec.slideUnit==TimeUnit.MILLISECOND && spec.slide<=500) spec.slide-percent
      else spec.slide-percent     
      outputevery=" output snapshot every "+interval(every,spec.slideUnit)
    }
    //  
    //if (spec.from==spec.slide && spec.fromUnit==spec.slideUnit)
    //  ".win:time_batch("+from+")"
    //else
    ".win:time("+from+")" 
  }
 
}

//** test class for static rdf **//
case class TripleData(@BeanProperty p1:String,@BeanProperty p2:String,@BeanProperty p3:String,
    @BeanProperty p4:String,@BeanProperty p5:String,@BeanProperty p6:String)

object SparqlGet {
  val logger=LoggerFactory.getLogger(getClass)
  def getvar(sol:QuerySolution,vari:String)=
    if (vari==null) null
    else sol.get(vari).toString

  def getData(query:String):Array[TripleData] ={
    println("best query: "+query)
  
    val model = ModelFactory.createDefaultModel
    val arp:RDFReader= model.getReader(RDFFormat.TTL)
	arp.read(model,getClass.getClassLoader.getResourceAsStream("sensordata.ttl"),"")    	   
    val qexec = QueryExecutionFactory.create(query, model)
    val res=qexec.execSelect
    val tt:Seq[String]=(1 to 10).map(_=>null)
    val vars:Seq[String]=res.getResultVars.toSeq++tt
    logger.debug("result vars: "+vars)
    val finali=res.map{sol=>      
      TripleData(getvar(sol,vars(0)),getvar(sol,vars(1)),
                 getvar(sol,vars(2)),getvar(sol,vars(3)),
                 getvar(sol,vars(4)),getvar(sol,vars(5)))          
    }.toArray
    println("triiii"+finali.mkString("--"))
    if (finali.isEmpty)
      Array(TripleData(null,null,null,null,null,null))
    else finali
      //TripleData("ISANGALL2","rata","topo","","",""),
        //TripleData("gata","tapa","topo","","",""))
  }
}