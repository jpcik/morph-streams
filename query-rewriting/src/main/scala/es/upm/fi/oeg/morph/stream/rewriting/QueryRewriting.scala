package es.upm.fi.oeg.morph.stream.rewriting

import java.io.StringReader
import java.net.URI
import java.util.Properties
import collection.JavaConversions._
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import org.apache.commons.lang.NotImplementedException
import com.google.common.collect.HashMultimap
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
import com.weiglewilczek.slf4s.Logging
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
import com.google.common.collect.Maps
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
import com.hp.hpl.jena.graph.query.Expression.Variable
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

class QueryRewriting(props: Properties,mapping:String) extends Logging {
  logger.debug("mapping is: "+mapping)
  private val reader = R2rmlReader(mapping)

  //private LinksetProcessor linker;
  //private boolean metadataMappings = false;
  private var bindings: ProjectionOp = null

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

  def translate(queryString: String): SourceQuery =
    translate(SparqlStream.parse(queryString))  

  def translate(query:StreamQuery): SourceQuery ={
    val opNew = translateToAlgebra(query)
    val pVars=getProjectList(query).map(a=>a._1->a._1).toMap
    transform(opNew,pVars)      
  }

  def transform(algebra: AlgebraOp,projectVars:Map[String,String]):SourceQuery={
    val adapter = props.getProperty("siq.adapter")
    val queryClass = props.getProperty("siq.adapter" + "." + adapter + ".query")
    logger.info("Using query adapter: "+queryClass)
    val resquery=
      if (!adapter.equals("sql")) {
        val theClass =try Class.forName(queryClass)
        catch {
          case e: ClassNotFoundException =>throw new QueryRewritingException("Unable to use adapter query", e)
        }
        try 
          theClass.getDeclaredConstructor(classOf[Map[String,String]]).newInstance(projectVars).asInstanceOf[SourceQuery]
        catch {
          case e: InstantiationException =>throw new QueryRewritingException("Unable to instantiate query", e)
          case e: IllegalAccessException =>throw new QueryRewritingException("Unable to instantiate query", e)
        }
      } 
      else new SqlQuery(projectVars)
        
    resquery.load(algebra)
    logger.info(resquery.serializeQuery)
    return resquery
  }
  /*
	private AlgebraOp partition(Op op)
	
	{
		if (props.get(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED).equals("false"))
			return null;
		if (op instanceof OpBGP)
		{
			List<Triple> triples = new ArrayList<Triple>();
			OpBGP bgp = (OpBGP)op;
			for (Triple t:bgp.getPattern().getList())
			{
				if (t.getSubject().getName().startsWith("prop"))
				{
					triples.add(t);
					continue;
				}
				else
					continue;
				/*
				if (t.getPredicate().getURI().equals(RDF.type.getURI()))
				{
					Collection<TriplesMap> tMaps = r2r.getTriplesMapForUri(t.getObject().getURI());
					if (tMaps.isEmpty())
					{
						System.out.println(t.getObject().getURI());
						triples.add(t);
					}
					else
						System.out.println(t.getObject().getURI());
				
					
				}
				else
				{
					Collection<PredicateObjectMap> poMaps = r2r.getPredicateObjectMapForUri(t.getPredicate().getURI());
					//Collection<RefPredicateObjectMap> rpoMaps = r2r.getRefPredicateObjectMapForUri(t.getPredicate().getURI());
					if (poMaps.isEmpty())// && rpoMaps.isEmpty())
					{
						System.out.println(t.getPredicate().getURI());
						triples.add(t);
					}
					else
						System.out.println(t.getPredicate().getURI());
				}*/
			}
			
			
			if (triples.isEmpty())				return null;
			
			Set<String> vars = new HashSet<String>();
			String select = " SELECT ";
			String where = " WHERE { ";
			
			for (Triple t:triples)
			{
				//where+= t.toString()+". ";
				if (t.getSubject().isVariable()) 
				{
					if (!vars.contains(t.getSubject().getName()))
					{
						vars.add(t.getSubject().getName());
						select+="?"+t.getSubject().getName()+" ";
					}
					where+="?"+t.getSubject().getName();
				}
				else
				{
					where+="<"+t.getSubject().getURI()+">";
				}
				where += " <"+t.getPredicate().getURI()+"> ";
				if (t.getObject().isVariable())
				{
					if ( !vars.contains(t.getObject().getName()))
					{
						vars.add(t.getObject().getName());
						select+="?"+t.getObject().getName()+" ";
					}
					where+="?"+t.getObject().getName()+" .";
				}
				else
				{
					where+="<"+t.getObject().getURI()+"> .";
				}
			}
			String querystring = select+where+" }";
			logger.debug(querystring);
			Query query = QueryFactory.create(querystring) ;

			QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8080/openrdf-workbench/repositories/wannengrat/query", query);
		
		OpRelation relation = new OpRelation("bindings");
		relation.setExtentName("constants");
		OpProjection p = new OpProjection("Bindings", relation );

		ValueSetXpr vs = new ValueSetXpr();

		ResultSet res = null;
		try
		{
		res = qexec.execSelect();
		}
		catch (ResultSetException e)
		{
			logger.info("No results from metadata");
			vs.getValueSet().add("NULL");
			//p.addExpression(t.getSubject().getName(), vs);
			return p;
		}
		for (String v:vars)
		{
			p.addExpression(v, new ValueSetXpr());
		}
		while (res.hasNext())
		{
			QuerySolution qs = res.next();
			logger.debug(qs);
			Iterator<String> it=qs.varNames();
			while (it.hasNext())
			{
				String var = it.next();
				ValueSetXpr vsx = (ValueSetXpr) p.getExpressions().get(var);
				vsx.getValueSet().add(qs.get(var).toString());
			}
			//p.addBinding(t.getSubject().getName(), qs.getResource("var").getURI());
			//vs.getValueSet().add(qs.getResource("var").getURI());
		}
		//p.addExpression(t.getSubject().getName(), vs);

		return p;	
			
		}
		else if (op instanceof OpProject)
		{
			OpProject proj = (OpProject)op;
			return partition(proj.getSubOp());
		}
		else if (op instanceof OpFilter)
		{
			OpFilter filter = (OpFilter)op;
			return partition(filter.getSubOp());
		}
		return null;
	}
	*/
  def translateToAlgebra(query: StreamQuery):AlgebraOp={
    val ini = System.currentTimeMillis      
    val op = Algebra.compile(query)
    val span1 = System.currentTimeMillis() - ini
    /*
    if (mappingUri != null) {
      //try reader.read(mappingUri)
      //catch {case e: Exception => throw new QueryRewritingException(e)}
    }*/
    //linker = new LinksetProcessor(mappingUri.toString());
    val span2 = System.currentTimeMillis() - ini;

    val binds = null //partition(op).asInstanceOf[OpProjection]
    this.bindings = binds;
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
        val cProj = new ProjectionOp("mainProjection",xprs, opo)

        //opNew.setSubOp(cProj)
        new RootOp(null,cProj)
      } else {
        new RootOp(null,null).build(opo)
      }
      val span3 = System.currentTimeMillis() - ini;

      opNew.display

      val opt = new QueryOptimizer
      val optimized=opt.staticOptimize(opNew)
      val span4 = System.currentTimeMillis() - ini;

      optimized.display

      //if (opNew.getSubOp==null) throw new Exception("Empty translated query throws no results")
      System.err.println(span1 + "-" + span2 + "-" + span3 + "-" + span4);
      optimized
  }

  private def processPOMap(t:Triple,poMap:PredicateObjectMap,tMap:TriplesMap,query:StreamQuery)={
    logger.debug("Graphs: " + poMap.graphMap)
    val graphstream=if (poMap.graphMap==null)
      tMap.subjectMap.graphMap 
      else poMap.graphMap
      
    val stream = query.getStream(
        if (graphstream!=null) graphstream.constant.asResource.getURI else null)

    val unary = createRelation(tMap, poMap.objectMap, stream)         
    if (t.getObject.isURI || t.getObject.isLiteral) {
      val selection = createSelection(t,poMap.objectMap,t.getObject.toString,unary)
      createProjection(t,tMap,poMap.objectMap,poMap, stream, selection)
    } 
    else createProjection(t, tMap, poMap.objectMap, poMap, stream,unary)  
    
  }
  
  def navigate(op: Op, query: StreamQuery): AlgebraOp ={
      if (op.isInstanceOf[OpBGP]) {
        val bgp = op.asInstanceOf[OpBGP]
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
            val ch=children.map{case proj:ProjectionOp=>
              proj.getRelation.id->proj}.toMap
            val union=new MultiUnionOp("multiunion",ch)
            
            //children.map{case proj:OpProjection=>              
            /*
              val map = HashMultimap.create[String, String]();
              map.put(proj.getId, proj.getId + proj.getRelation.getExtentName)
              union.index.put(proj.triple.getSubject.getName, map)
              if (proj.triple.getObject.isVariable && proj.link != null) {
                val map2 = HashMultimap.create[String, String]()
                map2.put(proj.link, proj.getId + proj.getRelation.getExtentName)
                union.index.put(proj.triple.getObject.getName, map2)*/
              //}
            //}
            
            opCurrent=union
          }
          else if (t.getPredicate.getURI.equals(RDF.typeProp.getURI)) {
            val tMaps = reader.filterBySubject(t.getObject.getURI) 
            skip = tMaps.isEmpty
            if (!skip) {
              opCurrent = null
              tMaps.foreach { tMap => //TODO adapt for multiple graphs
                logger.debug("Mapping graphs for: " + tMap.uri + " - " + tMap.subjectMap.graphMap)
                val graphs = tMap.subjectMap.graphMap
                val stream = 
                  if (graphs != null) 
                    query.getStream(tMap.subjectMap.graphMap.constant.asResource.getURI) 
                  else null
                val projection = createProjection(t, tMap, tMap.subjectMap, null, stream)

                if (opCurrent != null) opCurrent = union(opCurrent, projection)
                else opCurrent = projection
              }
            }
          } else {
            val poMaps = reader.filterByPredicate(t.getPredicate.getURI)
            skip = poMaps.isEmpty
            if (!skip) //continue;
            {
              var pro: AlgebraOp = null
              opCurrent = null

              poMaps.foreach {
                case (poMap, tMap) =>
                  pro=processPOMap(t,poMap,tMap,query)
                  if (opCurrent != null) opCurrent = union(opCurrent, pro)
                  else opCurrent = pro
              }
            }
          }
          if (!skip) {
            if (opCurrent == null) {
              return null
            }
            if (pra != null) pra = pra.build(opCurrent)
            else pra = opCurrent
          } else return null

        }
        pra;

      } else if (op.isInstanceOf[OpProject]) {
        val project = op.asInstanceOf[OpProject]
        val opo = navigate(project.getSubOp(), query)
        val xprs=project.getVars().map { vari =>
          val exp = UnassignedVarXpr //new VarXpr(vari.getVarName)
          (vari.getVarName, exp)
        }.toMap
        val proj = new ProjectionOp("mainProjection", xprs,opo)
        return proj;
      } else if (op.isInstanceOf[com.hp.hpl.jena.sparql.algebra.op.OpJoin]) {
        val opJoin = op.asInstanceOf[com.hp.hpl.jena.sparql.algebra.op.OpJoin]
        val l = navigate(opJoin.getLeft, query);
        val r = navigate(opJoin.getRight, query);
        val join = new InnerJoinOp(l, r)

        return join
        
      }else if (op.isInstanceOf[OpLeftJoin]) {
        val opJoin = op.asInstanceOf[OpLeftJoin]
        val l = navigate(opJoin.getLeft, query);
        val r = navigate(opJoin.getRight, query);
        val join = new LeftOuterJoinOp(l, r)

        return join;        
      }  else if (op.isInstanceOf[OpFilter]) {
        val filter = op.asInstanceOf[OpFilter]
        val it = filter.getExprs().iterator();
        val inner = navigate(filter.getSubOp(), query);

        val selXprs:Set[Xpr]=filter.getExprs.iterator.map{ex=>
          val expr = ex.asInstanceOf[ExprFunction2];
          val function = expr.getFunction
          val xpr =
            if (expr.getArg2().getConstant() != null)
              new BinaryXpr(function.getOpName,VarXpr(expr.getArg1.getVarName) ,ValueXpr(
                expr.getArg2.getConstant.toString))
            else
              new BinaryXpr(function.getOpName,VarXpr(expr.getArg1.getVarName),                
                VarXpr(expr.getArg2.getVarName))
          xpr
          //selection.addExpression(xpr);
          //function.
          //logger.debug("filter " + selection);
        }.toSet
        //logger.debug("Filter "+filter.toString());

        //selection.setSubOp(inner);
        val selection = new SelectionOp("selec", inner,selXprs);
        return selection;
      } else if (op.isInstanceOf[OpService]) {
        val service = op.asInstanceOf[OpService];
        
/*
        val sp = new OpSparql("service");
        val bgp = service.getSubOp().asInstanceOf[(OpBGP)]
        //List<Var> vars = new ArrayList<Var>();
        //vars.add(Var.alloc("sensor"));
        //OpProject p = new OpProject(bgp,vars);
        val q = OpAsQuery.asQuery(bgp);
        //q.addResultVar("pensor", new ExprVar("sensor"));
        //q.addResultVar("sensor");
        //QuerySolutionMap qs = new QuerySolutionMap();
        //qs.
        //QuerySolution initialBinding =;
        System.out.println(q.serialize());
        val e = QueryExecutionFactory.sparqlService("http://localhost:8080/openrdf-workbench/repositories/owlimDemo/query", q.serialize());
        val rs = e.execSelect();
        var qs: QuerySolution = null
        while (rs.hasNext()) {
          qs = rs.next();
          qs.get("sensor");
        }

        sp.sparql = service.getService().toString();
        sp.service = service.getName();
        return sp;*/
        throw new NotImplementedException("Query processing for SPARQL operation not supported: " + op.getClass.getName);
      } else if (op.isInstanceOf[OpDistinct]){
        return navigate(op.asInstanceOf[OpDistinct].getSubOp(),query)
      }
      else if (op.isInstanceOf[OpExtend]){
        val ext=op.asInstanceOf[OpExtend]
        logger.debug("Extend "+ext.getVarExprList().getExprs()+" "+ext.getSubOp)
        if (ext.getSubOp.isInstanceOf[OpGroup]){
        val group=ext.getSubOp.asInstanceOf[OpGroup]
         return new GroupOp(null,group.getGroupVars.getVars.map(v=>v.getVarName),
            group.getAggregators.map(a=>aggregator(extendReplace(ext,a.getVar.getVarName),a.getAggregator)).toMap,
            navigate(group.getSubOp,query))          
        }          
        else return navigate(ext.getSubOp,query)
      }
      else if (op.isInstanceOf[OpGroup]){
        val group=op.asInstanceOf[OpGroup]
        val g=new GroupOp(null,group.getGroupVars.getVars.map(v=>v.getVarName),
            group.getAggregators.map(a=>aggregator(a.getVar.getVarName,a.getAggregator)).toMap,
            navigate(group.getSubOp,query))
        return g
      }
      
      else {
        //Binding b = BindingFactory.create();
        //b.add(var, node)
        logger.info("None of above: " + op.getClass().getName());
        throw new NotImplementedException("Query processing for SPARQL operation not supported: " + op.getClass().getName());
      }

      //return null;	
  }
  
  private def extendReplace(extend:OpExtend,varname:String)={
    val alias=extend.getVarExprList.getExprs.find(e=>e._2.getVarName.equals(varname))
    if (alias.isDefined)
      alias.get._1.getVarName
    else varname
    
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
        val u = new MultiUnionOp("multiunion",ch)

        //u.getChildren().put(proj.getId + proj.getRelation.getExtentName, proj)
/*
        val map = HashMultimap.create[String, String]();

        map.put(proj.getId(), proj.getId() + proj.getRelation.getExtentName)
        u.index.put(proj.triple.getSubject.getName, map)
        if (proj.triple.getObject.isVariable && proj.link != null) {
          val map2 = HashMultimap.create[String, String]()
          map2.put(proj.link, proj.getId + proj.getRelation.getExtentName)
          u.index.put(proj.triple.getObject.getName, map2)
        }*/
        u
      }
    new MultiUnionOp(union.id,union.children++List(right.id+right.asInstanceOf[ProjectionOp].getRelation.extentName->right).toMap)
/*
      val proj = right.asInstanceOf[OpProjection];
      if (proj.getId() == null)
        logger.debug("is null" + proj);
      union.getChildren().put(proj.getId() + proj.getRelation().getExtentName(), proj); //+proj.getRelation().getExtentName(), proj);

      var map = union.index.get(proj.triple.getSubject().getName());
      if (map == null) {
        map = HashMultimap.create();
        union.index.put(proj.triple.getSubject().getName(), map);
      }
      map.put(proj.getId(), proj.getId() + proj.getRelation().getExtentName());

      if (proj.triple.getObject().isVariable() && proj.link != null) {
        map = union.index.get(proj.triple.getObject().getName());
        if (map == null) {
          map = HashMultimap.create();
          union.index.put(proj.triple.getObject().getName(), map);
        }
        map.put(proj.link, proj.getId() + proj.getRelation().getExtentName());
        logger.debug("index map" + proj.link + "--" + proj.getId());

      }*/
      //return union;
  }

  private def createSelection(xprs:Seq[(String,String,String)],subOp:AlgebraOp):UnaryOp={
    val varName=xprs.map(_._2).mkString("-")
    val selXprs:Set[Xpr]=xprs.map{x=>
      new BinaryXpr(x._2, VarXpr(x._1),ValueXpr(x._3))
      //selection.addExpression(xpr)
    }.toSet
    val selection = new SelectionOp(varName, subOp,selXprs)
    selection
  }
  
  private def createSelection(operation:String,varName:String,value:String,unary:AlgebraOp):UnaryOp={
    createSelection(Array((varName,operation,value)),unary)
  }
  
  private def createSelection(t: Triple, nMap: TermMap, value: String,unary:UnaryOp):UnaryOp={
    logger.debug("Create selection value: "+value )
    val vari = if (nMap.column == null)
      "localVar" + t.getPredicate.getLocalName + t.getSubject.getName
      else nMap.column;
    if (nMap.constant!=null && 
        nMap.constant.toString.equals(value))
      //createSelection("=",vari,value,unary)
      unary
    else null
  }

 
  private def projectionXprs(tripleNode:Node,oMap:TermMap):(String,Xpr)={   
    tripleNode match {
      case objVar:Var=>
        if (oMap.constant!=null){
          val op = new OperationXpr("constant",ValueXpr(oMap.constant.toString))
          (objVar.getName,op)
        }
        else if (oMap.column!=null){
          val varExp = new VarXpr(oMap.column)        
          (objVar.getName,varExp)
        }
        else if (oMap.template!=null){
          val columns=R2rmlUtils.extractTemplateVals(oMap.template)
          //val replace=new ReplaceXpr(oMap.template,columns.map(new VarXpr(_)))
          val fun=if (termType(oMap)==IRIType){
            new ReplaceXpr(oMap.template,columns.map(c=>new PercentEncodeXpr(VarXpr(c))))
          }
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
  
    
    
  private def createProjection(t:Triple,tMap:TriplesMap,nMap:TermMap,poMap:PredicateObjectMap,stream:ElementStreamGraph):ProjectionOp ={
    val unary = createRelation(tMap, nMap, stream)           
    createProjection(t, tMap, nMap, poMap, stream, unary)
  }

  private def createProjection(t:Triple, tMap:TriplesMap, nMap: TermMap, 
      poMap: PredicateObjectMap, stream: ElementStreamGraph, sub: UnaryOp): ProjectionOp =  {
      
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
      projectionXprs(t.getObject,subject)
    } else null
          
    val xprs=List(subjXprs)++List(objectXprs)++List(refXprs)
    logger.debug("projection Xprs: "+xprs)
    
    val relation = if (sub.isInstanceOf[RelationOp]) sub else sub.getRelation
    val projection = new ProjectionOp(id+"-"+relation.id,xprs.filter(_!=null).toMap, sub)
  
    //logger.debug("Created projection: "+projection.toString());
    return projection
  }

  private def createRelation(tMap:TriplesMap,nMap:TermMap, stream: ElementStreamGraph): UnaryOp ={
    var tableid = "";
    var extentName = "";
     
    val uri=new URI(tMap.uri).getFragment
    
    logger.debug("Creating relation: " + nMap +" table: " + tMap.logicalTable.sqlQuery);
    if (nMap!=null && nMap.constant != null) {
      tableid = nMap.constant.toString
      extentName = tMap.logicalTable.tableName
    } 
    else if (tMap.logicalTable.tableName != null) {
      tableid = tMap.logicalTable.tableName
      extentName = tableid;
    } 
    else if (tMap.logicalTable.sqlQuery != null) {
      tableid = SQLParser.tableAlias(tMap.logicalTable.sqlQuery)      
      extentName=tableid
    }    
    
    val relation =
      if (stream != null) {
        logger.debug("Create window: " + stream.getUri)
        val sw = stream.window.asInstanceOf[ElementTimeWindow]
        val wn=
          if (sw != null) {
            //win.setFromOffset(sw.getFrom.getOffset)
            //win.setFromUnit(sw.getFrom.getUnit)
            val (to,toU):(Long,TimeUnit)= if (sw.to != null) {
              (sw.to.time,sw.to.getUnit)
            }else (null,null)
            /*val (sl,slu):(Long,TimeUnit)= if (sw.getSlide != null) {
              (sw.getSlide.time,sw.getSlide.unit)
            } else ( null,null)*/
            /*val win = new WindowSpec("",sw.getFrom.offset,TimeUnit.toTimeUnit(sw.getFrom.unit),
                to,TimeUnit.toTimeUnit(toU),sl,TimeUnit.toTimeUnit(slu))*/
            val win = new WindowSpec("",sw.from.time,sw.from.unit,
                0,null,0,null)
            
            win
           
          } else null
          
          new WindowOp(tableid+uri, extentName,wn)
        } else {
          new RelationOp(tableid+"genId",extentName)
        }
      
      logger.debug("Created relation: " + relation.extentName)

      if (tMap.logicalTable.sqlQuery!=null){
        val cond=SQLParser.selections(tMap.logicalTable.sqlQuery)
        if (cond.isDefined){
          val sel=createSelection(cond.get,relation)
          //sel.setSubOp(relation)
          sel
        }
        else relation
      }        
      else relation
    }
    

}

class QueryCompilerException(msg:String,e:Throwable) extends Exception(msg,e)

class QueryRewritingException(msg:String,e:Throwable) extends QueryCompilerException(msg,e){
  def this(e:Throwable)=this(null,e)
}
