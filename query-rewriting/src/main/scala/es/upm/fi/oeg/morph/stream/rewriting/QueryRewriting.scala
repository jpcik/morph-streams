/**
   Copyright 2010-2013 Ontology Engineering Group, Universidad Politécnica de Madrid, Spain

   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
   compliance with the License. You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software distributed under the License is 
   distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
   See the License for the specific language governing permissions and limitations under the License.
**/

package es.upm.fi.oeg.morph.stream.rewriting

import java.io.StringReader
import java.net.URI
import java.util.Properties
import collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import org.apache.commons.lang.NotImplementedException
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpFilter
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.op.OpService
import com.hp.hpl.jena.sparql.algebra.Algebra
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.OpAsQuery
import com.hp.hpl.jena.sparql.expr.ExprFunction2
import es.upm.fi.oeg.morph.r2rml.PredicateObjectMap
import es.upm.fi.oeg.morph.r2rml.R2rmlReader
import es.upm.fi.oeg.morph.r2rml.TermMap
import es.upm.fi.oeg.morph.r2rml.TriplesMap
import es.upm.fi.oeg.morph.voc.RDF
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import es.upm.fi.oeg.morph.stream.algebra.InnerJoinOp
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import com.hp.hpl.jena.sparql.algebra.op.OpExtend
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin
import com.hp.hpl.jena.sparql.algebra.op.OpGroup
import es.upm.fi.oeg.morph.stream.algebra.GroupOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.AggXpr
import com.hp.hpl.jena.sparql.expr.aggregate._
import es.upm.fi.oeg.morph.stream.algebra.xpr._
import es.upm.fi.oeg.morph.r2rml.R2rmlUtils
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.RelationOp
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp
import es.upm.fi.oeg.morph.stream.algebra.UnaryOp
import es.upm.fi.oeg.morph.stream.algebra.WindowOp
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import es.upm.fi.oeg.morph.r2rml.SubjectMap
import es.upm.fi.oeg.morph.r2rml.RefObjectMap
import es.upm.fi.oeg.morph.r2rml.ObjectMap
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.graph.Node
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.morph.stream.algebra.WindowSpec
import es.upm.fi.oeg.sparqlstream.StreamQuery
import es.upm.fi.oeg.sparqlstream.StreamQueryFactory
import es.upm.fi.oeg.morph.common.TimeUnit
import es.upm.fi.oeg.sparqlstream.syntax.ElementTimeWindow
import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph
import es.upm.fi.oeg.sparqlstream.SparqlStream
import es.upm.fi.oeg.morph.r2rml.IRIType
import es.upm.fi.oeg.morph.r2rml.LiteralType
import es.upm.fi.oeg.morph.r2rml.RefObjectMap
import com.hp.hpl.jena.sparql.algebra.op.OpGraph
import es.upm.fi.oeg.morph.stream.algebra.PatternOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.ConstantXpr
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.query.Modifiers
import com.hp.hpl.jena.sparql.expr.Expr
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.ExprFunction0
import com.hp.hpl.jena.sparql.expr.NodeValue
import com.hp.hpl.jena.sparql.algebra.op.OpJoin
import com.hp.hpl.jena.sparql.algebra.op.OpUnion
import com.hp.hpl.jena.graph.Node_URI
import com.typesafe.config.ConfigFactory
import es.upm.fi.oeg.sparqlstream.StreamAlgebra
import es.upm.fi.oeg.sparqlstream.OpStreamGraph
import com.typesafe.config.ConfigException.Missing

class QueryRewriting(mapping:String,systemId:String="default") {
  private val logger= LoggerFactory.getLogger(this.getClass)
  val config=ConfigFactory.load.getConfig("morph.streams."+systemId)

  //val expanding=try config.getBoolean("rewriter.expanding") catch {case e:Missing=>false}
  val reasoning=try config.getBoolean("rewriter.reasoning") catch {case e:Missing=>false}
  val queryClass=try config.getString("adapter.query") catch {case e:Missing=>classOf[SqlQuery].getName}
  
  logger.debug("mapping is: "+mapping)
  private val reader = R2rmlReader(mapping)
  private var aliasCount=0
 
  private def getAlias={
    val value=aliasCount
    aliasCount+=1
    value
  }
  
  private def getProjectList(query: StreamQuery): Map[String,String]= {
    val map = new collection.mutable.HashMap[String, String]
    if (query.isConstructType) {
      val lt = query.getConstructTemplate.getTriples
      lt.foreach { t =>
        if (t.getSubject.isVariable)
          map.put(t.getSubject.getName.toLowerCase, null)
        if (t.getObject.isVariable)
          map.put(t.getObject.getName.toLowerCase, null)
      }
    }
    if (query.isSelectType)
      query.getProjectVars.foreach{ vari=>map.put(vari.getVarName.toLowerCase, null)}
    map.toMap
  }

  def translate(queryString: String): SourceQuery ={
    val parsed=SparqlStream.parse(queryString)
    //logger.debug("reordering query")
      translate(parsed)  
  }

  def translate(query:StreamQuery): SourceQuery ={   
    
    val expanded=if (!reasoning) query
      else OntologyRewriting.translate(query, config.getString("ontology"))
    val opNew = translateToAlgebra(QueryReordering.reorder(expanded))
    val pVars=getProjectList(query).map(a=>a._1->a._1).toMap
    transform(opNew,pVars,modifiers(query))      
  }

  def modifiers(query:StreamQuery)={
    val list=new collection.mutable.ArrayBuffer[Modifiers.OutputModifier]
    if (query.isDstream) list+=Modifiers.Dstream
    if (query.isIstream) list+=Modifiers.Istream
    if (query.isRstream) list+=Modifiers.Rstream

    list.toArray
  }
  
  def transform(algebra: AlgebraOp,projectVars:Map[String,String],
      mods:Array[Modifiers.OutputModifier]):SourceQuery={
    //val adapter = props.getProperty("siq.adapter")
      //if (!systemId.equals("default")) {
            //val queryClass = //config.getString("adapter" + "." + systemId + ".query")
    //logger.info("Using query adapter: "+queryClass)
        val theClass =try Class.forName(queryClass)
        catch {
          case e: ClassNotFoundException =>throw new QueryRewritingException("Unable to use adapter query "+queryClass, e)
        }
    val resquery=
        try 
          theClass.getDeclaredConstructor(classOf[AlgebraOp],
                                          classOf[Array[Modifiers.OutputModifier]])
            .newInstance(algebra,mods).asInstanceOf[SourceQuery]
        catch {
          case e: InstantiationException =>throw new QueryRewritingException("Unable to instantiate query", e)
          case e: IllegalAccessException =>throw new QueryRewritingException("Unable to instantiate query", e)
        }
      //} 
      //else new SqlQuery(algebra,mods)
        
    //resquery.load(algebra)
    logger.info(resquery.serializeQuery)
    return resquery
  }
  
  
  def translateToAlgebra(query: StreamQuery):AlgebraOp={
    val ini = System.currentTimeMillis      
    val op = StreamAlgebra.compile(query)
    val span1 = System.currentTimeMillis() - ini
    val span2 = System.currentTimeMillis() - ini;

    //val binds = null //partition(op).asInstanceOf[OpProjection]
    //this.bindings = binds;
    val opo = navigate(op, query);
      /*
      if (binds != null) {
        val mainPro = opo.asInstanceOf[ProjectionOp];
        mainPro.setSubOp(opo.build(binds));
      }*/
    val opNew=
      if (query.getConstructTemplate != null) { //TODO ugliest code ever, please refactor
        val tg = query.getConstructTemplate
        val xprs=tg.getTriples.map { tt =>
          var vari = ""
          if (tt.getSubject.isVariable) 
            vari = tt.getSubject.getName          
          if (tt.getObject.isVariable) 
            vari = tt.getObject.getName
          
          val exp = new VarXpr(vari)
          (vari, exp)
        }.toMap
        val cProj = new ProjectionOp(xprs, opo,query.isDistinct)

        //opNew.setSubOp(cProj)
        new RootOp(null,cProj)
      } else {
        new RootOp(null,null).build(opo)
      }
      val span3 = System.currentTimeMillis() - ini;

      logger.debug("before optimization")
      opNew.display

      val opt = new QueryOptimizer
      val optimized=opt.staticOptimize(opNew)
      val span4 = System.currentTimeMillis() - ini;

      optimized.display

      if (optimized.asInstanceOf[UnaryOp].subOp==null)
        throw new QueryRewritingException("Rewriting resulted in empty query. The query cannot be answered given the speecified mappings.",null)
      
      //if (opNew.getSubOp==null) throw new Exception("Empty translated query throws no results")
      System.err.println(span1 + "-" + span2 + "-" + span3 + "-" + span4);
      optimized
  }

  private def processPOMap(t:Triple,poMap:PredicateObjectMap,tMap:TriplesMap,query:StreamQuery)={
    logger.debug("Processing triple: "+t)
    logger.debug("Graphs: " + poMap.graphMap)
    val graphstream=if (poMap.graphMap==null)
      tMap.subjectMap.graphMap 
      else poMap.graphMap
      
    val stream = query.getStream(
        if (graphstream!=null) graphstream.constant.asResource.getURI else null)

    val unary = createRelation(tMap, poMap.objectMap, stream)           
    if (t.getObject.isURI || t.getObject.isLiteral) {
      val oMap=if (poMap.objectMap==null)  reader.triplesMaps(poMap.refObjectMap.parentTriplesMap).subjectMap
        else poMap.objectMap
      val selection = createSelection(t,oMap,t.getObject.toString,unary)
      createProjection(t,tMap,poMap.objectMap,poMap, selection)
    } 
    else if (t.getSubject.isURI){
      val selection = createSelection(t,tMap.subjectMap,t.getSubject.toString,unary)
      createProjection(t,tMap,poMap.objectMap,poMap, selection)
    }
    else createProjection(t, tMap, poMap.objectMap, poMap,unary)  
    
  }
  
  @deprecated("Use processBgp","1.0.6")
  private def processOldBgp(bgp:OpBGP,query:StreamQuery):AlgebraOp={
      var opCurrent: AlgebraOp = null
      var pra: AlgebraOp = null
      val triples = bgp.getPattern.getList
      var skip = false
      triples.foreach { t =>
        skip = false
        if (t.getPredicate.isVariable){
          val poMaps=reader.allPredicates
          val children=poMaps.map{case (poMap, tMap) =>
            processPOMap(t,poMap,tMap,query)
          } 
          val ch=children.map{case proj:ProjectionOp=>proj.getRelation.id->proj}.toMap          
          opCurrent=new MultiUnionOp(ch)
        }
        else if (t.getPredicate.getURI.equals(RDF.typeProp.getURI)) {
          val tMaps = reader.filterBySubject(t.getObject.getURI) 
          skip = tMaps.isEmpty
          if (!skip) {
            opCurrent = null
            tMaps.foreach { tMap => //TODO adapt for multiple graphs
              logger.debug("Mapping graphs for: " + tMap.uri + " - " + tMap.subjectMap.graphMap)
              val projection = createProjection(t, tMap, tMap.subjectMap, null, query)
              if (opCurrent != null) opCurrent = union(opCurrent, projection)
              else opCurrent = projection
            }
          }
        } 
        else {
          val poMaps = reader.filterByPredicate(t.getPredicate.getURI)
          logger.debug("Tmaps for "+t.getPredicate+" "+poMaps.size )
          skip = poMaps.isEmpty
          if (!skip) {
            var pro: AlgebraOp = null
            opCurrent = null
            poMaps.foreach {
              case (poMap, tMap) =>
                pro=processPOMap(t,poMap,tMap,query)
                logger.debug("after poMap "+pro)
                if (opCurrent != null) opCurrent = union(opCurrent, pro)
                else opCurrent = pro
            }
          }
        }
        if (!skip) {
          if (opCurrent == null) {
            throw new Exception("too bad")
            return null
          }
          if (pra != null) pra = pra.build(opCurrent)
          else pra = opCurrent
        } 
        else {
          throw new Exception("bad")
          return null
        }

      }
      logger.debug("We get this "+pra)
      pra

  }

  private def processBgp(bgp:OpBGP,query:StreamQuery):AlgebraOp={       
    val ops=bgp.getPattern.getList.map { t =>
      val oper=
       if (t.getPredicate.isVariable){
         val poMaps=reader.allPredicates
         val children=poMaps.map{case (poMap, tMap) =>
           processPOMap(t,poMap,tMap,query)
         } 
         val ch=children.map{case proj:ProjectionOp=>proj.getRelation.id->proj}.toMap          
         new MultiUnionOp(ch)
       }
       else if (t.getPredicate.getURI.equals(RDF.typeProp.getURI)) {
         val tMaps = reader.filterBySubject(t.getObject.getURI) 
         val els=  tMaps.map { tMap => //TODO adapt for multiple graphs
           logger.debug("Mapping graphs for: " + tMap.uri + " - " + tMap.subjectMap.graphMap)
           createProjection(t, tMap, tMap.subjectMap, null, query)           
         } 
         new MultiUnionOp(els.map(p=>p.id->p).toMap).simplify
       } 
       else {
         val poMaps = reader.filterByPredicate(t.getPredicate.getURI)
         val els=poMaps.map {
           case (poMap, tMap) =>
             val pro=processPOMap(t,poMap,tMap,query)
             logger.debug("after poMap "+pro)
             pro
         }.filter(_!=null)
         new MultiUnionOp(els.map{p=>p.id->p}.toMap).simplify
       }
            
      logger debug "Step subpattern: "+oper
      oper
    }
    val conj=ops.reduce((a,b)=>conjunction(a,b))    
    logger.debug("We get this "+conj)
    conj
  }

  private def conjunction(op1:AlgebraOp,op2:AlgebraOp)={
    if (op1==null || op2==null) null
    else op1.build(op2)
  }
  
  def navigate(op:Op, query:StreamQuery):AlgebraOp=op match{ 
    case bgp:OpBGP=>      
      processBgp(bgp, query)
    case project:OpProject=>        
      val opo = navigate(project.getSubOp, query)
      val xprs=project.getVars.map(vari=> (vari.getVarName, UnassignedVarXpr)).toMap
      new ProjectionOp(xprs,opo,query.isDistinct)
      
    case opJoin:OpJoin=>     
      val l = navigate(opJoin.getLeft, query)
      val r = navigate(opJoin.getRight, query)
      new InnerJoinOp(l, r)        
        
    case opJoin:OpLeftJoin=>        
      val l = navigate(opJoin.getLeft, query)
      val r = navigate(opJoin.getRight, query)
      new LeftOuterJoinOp(l, r)
              
    case filter:OpFilter=>        
      val inner = navigate(filter.getSubOp,query)
      val selXprs:Set[Xpr]=filter.getExprs.iterator.map(ex=>decodeXpr(ex)).toSet
      new SelectionOp("selec", inner,selXprs).simplify      
    case service:OpService=>
      throw new NotImplementedException("Query processing for SPARQL operation not supported: " + op.getClass.getName)
    case distinct:OpDistinct=>
      navigate(distinct.getSubOp,query)
    case ext:OpExtend=>  
      logger.debug("Extend "+ext.getVarExprList.getExprs+" "+ext.getSubOp)
      ext.getSubOp match {
        case group:OpGroup=>
          new GroupOp(null,group.getGroupVars.getVars.map(v=>v.getVarName),
            group.getAggregators.map(a=>aggregator(extendReplace(ext,a.getVar.getVarName),a.getAggregator)).toMap,
            navigate(group.getSubOp,query))          
        case _=> navigate(ext.getSubOp,query)
      }
    case group:OpGroup=>     
      new GroupOp(null,group.getGroupVars.getVars.map(v=>v.getVarName),
            group.getAggregators.map(a=>aggregator(a.getVar.getVarName,a.getAggregator)).toMap,
            navigate(group.getSubOp,query))      
    case union:OpUnion=> 
      val l = navigate(union.getLeft, query)
      val r = navigate(union.getRight, query)
      new MultiUnionOp(Seq(l,r).filter(_!=null).map(opr=>opr.id->opr).toMap).simplify
    case graph:OpStreamGraph=>       
      navigate(graph.getSubOp,query)
    case graph:OpGraph=>       
      val bgp=graph.getSubOp.asInstanceOf[OpBGP]
      val vars=
        bgp.getPattern().map{tr=>
          val obj=tr.getObject match{case v:Var=>v.getVarName}
          val sub=tr.getSubject match{case v:Var=>v.getVarName}
          Array(sub,obj)
        }.flatten.toSet
      val varMap=vars.zipWithIndex.map(t=>t._1->VarXpr("p"+(t._2+1))).toMap
      val pattern=new PatternOp("sparql",graph.getName,bgp)
      new ProjectionOp(varMap,pattern,false)
      
    case _=>
      logger.info("None of above: " + op.getClass.getName)
      throw new NotImplementedException("Query processing for SPARQL operation not supported: " + op.getClass.getName)
  }
  
  private def extendReplace(extend:OpExtend,varname:String)={
    val alias=extend.getVarExprList.getExprs.find(e=>e._2.getVarName.equals(varname))
    if (alias.isDefined)
      alias.get._1.getVarName
    else varname
    
  }

  private def decodeXpr(ex:Expr):Xpr=ex match{
    case expr:ExprFunction2=>
      val function = expr.getFunction
      BinaryXpr(function.getOpName,decodeXpr(expr.getArg1),decodeXpr(expr.getArg2))      
    case vxpr:ExprVar=>VarXpr(vxpr.getVarName)
    case const:ExprFunction0=>ValueXpr(const.getConstant.toString)
    case vale:NodeValue=>ValueXpr(vale.asString)
    case _=>throw new NotImplementedException("Not implemented expression: "+ex)
      
  }
  
  private def aggregator(varName:String,agg:Aggregator)={
    val op=agg match  {
      case max:AggMax=>MaxXpr
      case min:AggMin=>MinXpr
      case avg:AggAvg=>AvgXpr
      case count:AggCount=>CountXpr
      case sum:AggSum=>SumXpr
    }
    (varName,new AggXpr(op,agg.getExpr.getVarName))
  }
  
  private def union(left: AlgebraOp, right: AlgebraOp): AlgebraOp ={
    val union=
      if (left.isInstanceOf[MultiUnionOp])
        left.asInstanceOf[MultiUnionOp]
      else {
        val proj = left.asInstanceOf[ProjectionOp]
        val ch=List(proj.id + proj.getRelation.extentName-> proj).toMap
        new MultiUnionOp(ch)
      }
    val rightTerm=if (right==null) Map()
      else List(right.id+right.asInstanceOf[ProjectionOp].getRelation.extentName->right).toMap
    new MultiUnionOp(union.children++rightTerm).simplify
  }

  private def createSelection(xprs:Seq[(String,String,String)],subOp:AlgebraOp):UnaryOp={
    val varName=xprs.map(_._2).mkString("-")
    val selXprs:Set[Xpr]=xprs.map{x=>
      new BinaryXpr(x._2, VarXpr(x._1),ValueXpr(x._3))
    }.toSet
    new SelectionOp(varName, subOp,selXprs)    
  }
  
  private def createSelection(t: Triple, nMap: TermMap, value: String,unary:UnaryOp):UnaryOp={
    logger.debug("Create selection value: "+value )
    val vari = if (nMap.column == null)      
      "localVar" + t.getPredicate.getLocalName +value//+ t.getSubject.getName
      else nMap.column
    if (nMap.column!=null){
      createSelection(Array((vari,"=",value)),unary)
    }
    else if (nMap.constant!=null && 
        nMap.constant.toString.equals(value))
      //createSelection("=",vari,value,unary)
      unary
    else if (nMap.template!=null){
       val columns=R2rmlUtils.extractTemplateVals(nMap.template)
       val rep=new ReplaceXpr(nMap.template,columns.map(VarXpr(_)))
       val setXpr:Set[Xpr]=Set(BinaryXpr("=",rep,new ConstantXpr(value)))
       new SelectionOp("selct",unary,setXpr)
    } 
    else null
  }

  private def projectionXprs(tripleNode:Node,roMap:RefObjectMap,sMap:SubjectMap):(String,Xpr)={
    tripleNode match{
      case objVar:Var=>        
        //if (roMap.joinCondition!=null)
          //(objVar.getName,new VarXpr(roMap.joinCondition.child))
        //else 
        projectionXprs(objVar,sMap)
          //(objVar.getName,new VarXpr("identity"))
      case usi:Node_URI=>null  
      case _=> throw new Exception("Unsupported triple pattern: "+tripleNode)
    }
  }
 
  private def projectionXprs(tripleNode:Node,oMap:TermMap):(String,Xpr)={   
    tripleNode match {
      case objVar:Var=>
        if (oMap.constant!=null)        
          (objVar.getName,new ConstantXpr(oMap.constant.toString))
        else if (oMap.column!=null)          
          (objVar.getName,VarXpr(oMap.column))        
        else if (oMap.template!=null){
          val columns=R2rmlUtils.extractTemplateVals(oMap.template)
          val fun=if (termType(oMap)==IRIType)
            new ReplaceXpr(oMap.template,columns.map(c=>new PercentEncodeXpr(VarXpr(c))))          
          else new ReplaceXpr(oMap.template,columns.map(VarXpr(_)))                    
          (objVar.getName,fun)
        }
        else null
      case _=>null
    }
  }

  private def termType(term:TermMap)=
    if (term.termType == null) term match {
      case subj:SubjectMap=>IRIType
      case obj:ObjectMap=>
        if (term.column!=null || obj.dtype!=null || obj.language!=null) LiteralType
        else IRIType
    }
    else term.termType
  
    
    
  private def createProjection(t:Triple,tMap:TriplesMap,nMap:TermMap,poMap:PredicateObjectMap,query:StreamQuery):ProjectionOp ={
    val graphs = tMap.subjectMap.graphMap
    val stream = 
      if (graphs != null) query.getStream(tMap.subjectMap.graphMap.constant.asResource.getURI) 
      else null
    val unary = createRelation(tMap, nMap, stream)           
    createProjection(t, tMap, nMap, poMap, unary)
  }

  private def createProjection(t:Triple, tMap:TriplesMap, nMap: TermMap, 
      poMap: PredicateObjectMap, sub: UnaryOp): ProjectionOp =  {
    if (sub==null) null
    else {
    val nMapUri = tMap.uri

    logger.debug("Creating projection for "+nMap)
    val id = 
      if (tMap.subjectMap.constant != null) tMap.subjectMap.constant.toString
      else if (tMap.subjectMap.column != null) tMap.subjectMap.column
      else tMap.subjectMap.template
       
    val objectXprs= nMap match{
      case subjectMap:SubjectMap=>projectionXprs(t.getSubject,subjectMap)
      case objectMap:ObjectMap=>projectionXprs(t.getObject,objectMap)
      case _=>null
    }
    val subjXprs=if (poMap!=null) {
      projectionXprs(t.getSubject,tMap.subjectMap)
    }
    else null
    
    val refXprs=if (poMap!=null && poMap.refObjectMap!=null){
      val roMap=poMap.refObjectMap
      val subject = reader.triplesMaps(roMap.parentTriplesMap).subjectMap
      projectionXprs(t.getObject,roMap,subject)
    } else null
          
    val xprs=List(subjXprs)++List(objectXprs)++List(refXprs)
    logger.debug("projection Xprs: "+xprs)
    
    
    val relation = if (sub.isInstanceOf[RelationOp]) sub else sub.getRelation
    val projection = new ProjectionOp(xprs.filter(_!=null).toMap, sub,false)
  
    //logger.debug("Created projection: "+projection.toString());
    projection
    }
  }

  private def createRelation(tMap:TriplesMap,nMap:TermMap, stream: ElementStreamGraph): UnaryOp ={     
    val uri=new URI(tMap.uri).getFragment
    
    logger.debug("Creating relation: " + nMap +" table: " + tMap.logicalTable)
    logger.debug("pks: "+tMap.logicalTable.pk)
    val (tableid,extentName)=
      if (nMap!=null && nMap.constant != null) 
        (nMap.constant.toString,tMap.logicalTable.tableName)
      else if (tMap.logicalTable.tableName != null) 
        (tMap.logicalTable.tableName,tMap.logicalTable.tableName)
      else if (tMap.logicalTable.sqlQuery != null) {
        val parsedTableid = SQLParser.tableAlias(tMap.logicalTable.sqlQuery)
        (parsedTableid,parsedTableid)     
      }    
      else throw new QueryRewritingException("Unable to create relation "+tMap.toString,null)
    
    val relation =
      if (stream != null) {
        logger.debug("Create window: " + stream.getUri)
        val sw = stream.window.asInstanceOf[ElementTimeWindow]
        val wn=
          if (sw != null) {         
            val (slide,slunit)=if (sw.slide==null) (0L,null) 
              else (sw.slide.time,sw.slide.unit)
            new WindowSpec("",sw.from.time,sw.from.unit,0,null,slide,slunit)                                   
          } 
          else null        
          new WindowOp(tableid+getAlias+uri, extentName,tMap.logicalTable.pk,wn)
      } 
      else 
        new RelationOp(tableid+"genId",extentName,tMap.logicalTable.pk)
        
      
      logger.debug("Created relation: " + relation.extentName)

      if (tMap.logicalTable.sqlQuery!=null){
        val cond=SQLParser.selections(tMap.logicalTable.sqlQuery)
        if (cond.isDefined)
          createSelection(cond.get,relation)                 
        else relation
      }        
      else relation
    }
    

}

class QueryCompilerException(msg:String,e:Throwable) extends Exception(msg,e)

class QueryRewritingException(msg:String,e:Throwable) extends QueryCompilerException(msg,e){
  def this(e:Throwable)=this(null,e)
}
