package es.upm.fi.oeg.morph.stream.rewriting

import java.io.StringReader
import java.net.URI
import java.util.Properties
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
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEqlQuery
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr
import es.upm.fi.dia.oeg.integration.algebra.OpInterface
import es.upm.fi.dia.oeg.integration.algebra.OpJoin
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion
import es.upm.fi.dia.oeg.integration.algebra.OpProjection
import es.upm.fi.dia.oeg.integration.algebra.OpRelation
import es.upm.fi.dia.oeg.integration.algebra.OpRoot
import es.upm.fi.dia.oeg.integration.algebra.OpSelection
import es.upm.fi.dia.oeg.integration.algebra.OpSparql
import es.upm.fi.dia.oeg.integration.algebra.OpUnary
import es.upm.fi.dia.oeg.integration.algebra.OpWindow
import es.upm.fi.dia.oeg.integration.algebra.Window
import es.upm.fi.dia.oeg.integration.translation.QueryTranslationException
import es.upm.fi.dia.oeg.integration.QueryExecutor
import es.upm.fi.dia.oeg.integration.SourceQuery
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException
import es.upm.fi.dia.oeg.r2o.plan.Attribute
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementStream
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementTimeWindow
import es.upm.fi.dia.oeg.sparqlstream.StreamQuery
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory
import es.upm.fi.oeg.morph.r2rml.PredicateObjectMap
import es.upm.fi.oeg.morph.r2rml.R2rmlReader
import es.upm.fi.oeg.morph.r2rml.TermMap
import es.upm.fi.oeg.morph.r2rml.TriplesMap
import es.upm.fi.oeg.morph.voc.RDF
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct

class QueryRewriting(props: Properties) extends Logging {

  //private val r2r: R2RModel = new R2RModel
  private val reader = new R2rmlReader

  //private LinksetProcessor linker;
  //private boolean metadataMappings = false;
  private var bindings: OpProjection = null
  /*
	public QueryTranslator(Properties props)
	{
		this.props = props;
		r2r = new R2RModel();		
		//if (props.getProperty(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED).equals("true"))
		//	metadataMappings = true;
	}*/
  /*
	public QueryTranslator(Properties props, String mappingEndopint)
	{
		this.props = props;
		r2r = new R2RModel(mappingEndopint);
		
		//reader=R2rmlReader.apply(mappingEndopint);//testPath+"/"+name+"/"+ tc.mappingDoc)
		//if (props.getProperty(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED).equals("true"))
		//	metadataMappings = true;
	}*/
  /*
	public void getConstruct(String queryString)
	{
		StreamQuery query = (StreamQuery)StreamQueryFactory.create(queryString);	
	}*/

  def getProjectList(queryString: String): Map[String, Attribute] =
    {
      //HashMap<String,Attribute> map = new HashMap<String, Attribute>();
      val map = new collection.mutable.HashMap[String, Attribute]
      val query = StreamQueryFactory.create(queryString).asInstanceOf[StreamQuery]

      //query.getProjectVars()
      if (query.isConstructType()) {
        val lt = query.getConstructTemplate().getTriples()
        lt.foreach { t =>
          if (t.getSubject().isVariable())
            map.put(t.getSubject().getName().toLowerCase(), null)
          if (t.getObject().isVariable())
            map.put(t.getObject().getName().toLowerCase(), null)
        }
      }
      if (query.isSelectType())

        query.getProjectVars().foreach //.getResultVars())
        { vari =>
          map.put(vari.getVarName().toLowerCase(), null);
        }
      map.toMap
    }

  def translate(queryString: String, mappingUri: URI): SourceQuery = // throws  QueryTranslationException 
    {
      val opNew = translateToAlgebra(queryString, mappingUri);
      val sourcequery = transform(opNew);
      sourcequery.setOriginalQuery(queryString);
      return sourcequery;

    }

  def transform(algebra: OpInterface): SourceQuery = // throws QueryTranslationException
    {
      val adapter = props.getProperty("siq.queryexecutor.adapter");
      val queryClass = props.getProperty("siq.queryexecutor.adapter" + "." + adapter + ".query");
      logger.info("Using query adapter: "+queryClass)
      val resquery =
        if (!adapter.equals("snee")) {
          val theClass =
            try Class.forName(queryClass)
            catch {
              case e: ClassNotFoundException =>
                throw new QueryTranslationException("Unable to use adapter query", e)
            }

          try {
            theClass.newInstance().asInstanceOf[SourceQuery];
          } catch {
            case e: InstantiationException =>
              throw new QueryTranslationException("Unable to instantiate query", e)
            case e: IllegalAccessException =>
              throw new QueryTranslationException("Unable to instantiate query", e)
          }
        } else
          new SNEEqlQuery
      resquery.load(algebra)
      logger.info(resquery.serializeQuery());
      return resquery;

    }
  /*
	private OpInterface partition(Op op)
	
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
  def translateToAlgebra(queryString: String, mappingUri: URI): OpInterface = //  throws  QueryTranslationException
    {
      val ini = System.currentTimeMillis();
      val query = StreamQueryFactory.create(queryString).asInstanceOf[StreamQuery]
      val op = Algebra.compile(query);
      val span1 = System.currentTimeMillis() - ini;
      if (mappingUri != null) {
        try {
          reader.read(mappingUri);
        } catch {
          case e: InvalidR2RDocumentException => throw new QueryTranslationException(e)
          case e: InvalidR2RLocationException => throw new QueryTranslationException(e);
        }
      }
      //linker = new LinksetProcessor(mappingUri.toString());
      val span2 = System.currentTimeMillis() - ini;

      val binds = null //partition(op).asInstanceOf[OpProjection]
      this.bindings = binds;
      val opo = navigate(op, query);
      if (binds != null) {
        val mainPro = opo.asInstanceOf[OpProjection];
        mainPro.setSubOp(opo.build(binds));
      }
      val opNew = new OpRoot(null);

      if (query.getConstructTemplate() != null) { //TODO ugliest code ever, please refactor
        val cProj = new OpProjection("mainProjection", opo);
        //TemplateGroup tg = (TemplateGroup)query.getConstructTemplate();
        val tg = query.getConstructTemplate();
        //for (Template temp:tg.getTemplates())
        tg.getTriples.foreach { tt =>
          var vari = "";
          //TemplateTriple tt = (TemplateTriple)temp;
          if (tt.getSubject().isVariable()) {
            vari = tt.getSubject().getName();
            val exp = new VarXpr(vari);
            cProj.addExpression(vari, exp);
          }
          if (tt.getObject().isVariable()) {
            vari = tt.getObject().getName();
            val exp = new VarXpr(vari);
            cProj.addExpression(vari, exp);
          }

        }

        opNew.setSubOp(cProj);
      } else {
        opNew.build(opo);
      }
      val span3 = System.currentTimeMillis() - ini;

      opNew.display();

      val opt = new QueryOptimizer
      opt.staticOptimize(opNew);
      val span4 = System.currentTimeMillis() - ini;

      opNew.display();

      System.err.println(span1 + "-" + span2 + "-" + span3 + "-" + span4);
      return opNew;
    }

  def navigate(op: Op, query: StreamQuery): OpInterface =
    {
      if (op.isInstanceOf[OpBGP]) {
        val bgp = op.asInstanceOf[OpBGP]
        var opCurrent: OpInterface = null;
        var pra: OpInterface = null;
        val triples = bgp.getPattern().getList()
        var skip = false
        triples.foreach { t =>
          skip = false
          if (t.getPredicate.getURI.equals(RDF.typeProp.getURI)) {
            val tMaps = reader.filterBySubject(t.getObject.getURI) //r2r.getTriplesMapForUri(t.getObject().getURI());
            skip = tMaps.isEmpty
            if (!skip) {
              opCurrent = null;
              tMaps.foreach { tMap => //TODO adapt for multiple graphs
                logger.debug("Mapping graphs for: " + tMap.uri + " - " + tMap.subjectMap.graphMap)
                val graphs = tMap.subjectMap.graphMap
                val stream =
                  if (graphs != null) // && graphs.size() > 0)
                    query.getStream(tMap.subjectMap.graphMap.constant.asResource.getURI) //.getSubjectMap().getGraphSet().iterator().next());
                  else null
                val projection = createProjection(t, tMap, tMap.subjectMap, null, stream);

                if (opCurrent != null) opCurrent = union(opCurrent, projection);
                else opCurrent = projection;
              }
            }
          } else {
            val poMaps = reader.filterByPredicate(t.getPredicate().getURI);
            skip = poMaps.isEmpty
            if (!skip) //continue;
            {
              var pro: OpInterface = null;
              opCurrent = null;

              poMaps.foreach {
                case (poMap, tMap) =>
                  logger.debug("Graphs: " + poMap.graphMap);
                  val graphstream=if (poMap.graphMap==null)tMap.subjectMap.graphMap else poMap.graphMap
                  
                  val stream = query.getStream(if (graphstream!=null) graphstream.constant.asResource.getURI else null)

                  if (t.getObject().isURI()) {
                    val selection = createSelection(t, poMap.objectMap, t.getObject().getURI());

                    pro = createProjection(t, tMap, poMap.objectMap, poMap, stream, selection);
                    pro.build(selection);
                    if (opCurrent != null) opCurrent = union(opCurrent, pro);
                    else opCurrent = pro;

                  } else {
                    
                    pro = createProjection(t, tMap, poMap.objectMap, poMap, stream);
                    if (opCurrent != null) opCurrent = union(opCurrent, pro);
                    else opCurrent = pro;
                  }

              }
            }
          }
          if (!skip) {
            if (opCurrent == null) {
              return null;
            }
            if (pra != null) pra = pra.build(opCurrent);
            else pra = opCurrent;
          }

        }
        pra;

      } else if (op.isInstanceOf[OpProject]) {
        val project = op.asInstanceOf[OpProject];
        val proj = new OpProjection("mainProjection", null);
        project.getVars().foreach { vari =>
          val exp = new VarXpr(vari.getVarName());
          proj.addExpression(vari.getVarName(), exp);
        }
        val opo = navigate(project.getSubOp(), query);
        proj.setSubOp(opo);
        return proj;
      } else if (op.isInstanceOf[com.hp.hpl.jena.sparql.algebra.op.OpJoin]) {
        val opJoin = op.asInstanceOf[com.hp.hpl.jena.sparql.algebra.op.OpJoin]
        val l = navigate(opJoin.getLeft(), query);
        val r = navigate(opJoin.getRight(), query);
        val join = new OpJoin("whatHere", l, r);

        return join;
      } else if (op.isInstanceOf[OpFilter]) {
        val filter = op.asInstanceOf[OpFilter]
        val it = filter.getExprs().iterator();
        val selection = new OpSelection("selec", null);
        while (it.hasNext()) {
          val expr = it.next().asInstanceOf[ExprFunction2];
          val function = expr.getFunction();
          val xpr =
            if (expr.getArg2().getConstant() != null)
              BinaryXpr.createFilter(expr.getArg1().getVarName(), function.getOpName(),
                expr.getArg2().getConstant().toString());
            else
              BinaryXpr.createFilter(new VarXpr(expr.getArg1().getVarName()),
                function.getOpName(),
                new VarXpr(expr.getArg2().getVarName()));
          selection.addExpression(xpr);
          //function.
          logger.debug("filter " + selection);
        }
        //logger.debug("Filter "+filter.toString());

        val inner = navigate(filter.getSubOp(), query);
        selection.setSubOp(inner);
        return selection;
      } else if (op.isInstanceOf[OpService]) {
        val service = op.asInstanceOf[OpService];

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
        return sp;
      } else if (op.isInstanceOf[OpDistinct]){
        return navigate(op.asInstanceOf[OpDistinct].getSubOp(),query)
      }
      
      else {
        //Binding b = BindingFactory.create();
        //b.add(var, node)
        logger.info("None of above: " + op.getClass().getName());
        throw new NotImplementedException("Query processing for SPARQL operation not supported: " + op.getClass().getName());
      }

      //return null;	
    }

  private def union(left: OpInterface, right: OpInterface): OpInterface =
    {
      //OpUnion union1 = new OpUnion(left, right);
      //if (true) return union1;

      var union: OpMultiUnion = null;
      if (left.isInstanceOf[OpMultiUnion])
        union = left.asInstanceOf[OpMultiUnion];
      else {
        union = new OpMultiUnion("multiunion");
        val proj = left.asInstanceOf[OpProjection];

        union.getChildren().put(proj.getId() + proj.getRelation().getExtentName(), proj); //+proj.getRelation().getExtentName(), proj);

        val map = HashMultimap.create[String, String]();

        map.put(proj.getId(), proj.getId() + proj.getRelation().getExtentName());
        union.index.put(proj.triple.getSubject().getName(), map);
        if (proj.triple.getObject().isVariable() && proj.link != null) {
          val map2 = HashMultimap.create[String, String]();
          map2.put(proj.link, proj.getId() + proj.getRelation().getExtentName());
          union.index.put(proj.triple.getObject().getName(), map2);
        }
      }

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

      }
      return union;
    }

  private def createSelection(xprs:Seq[(String,String,String)]):OpSelection={
    val varName=xprs.map(_._2).mkString("-")
    val selection = new OpSelection(varName, null)
    xprs.foreach{x=>
      val xpr = BinaryXpr.createFilter(x._1, x._2, x._3)
      selection.addExpression(xpr)
    }
    selection
  }
  
  private def createSelection(operation:String,varName:String,value:String):OpSelection={
    createSelection(Array((varName,operation,value)))
  }
  
  private def createSelection(t: Triple, nMap: TermMap, value: String):OpSelection={
      val vari = if (nMap.column == null)
        "localVar" + t.getPredicate().getLocalName() + t.getSubject().getName()
      else nMap.column;
      createSelection("=",vari,value)
    }

  private def createProjection(t: Triple, tMap: TriplesMap, nMap: TermMap, poMap: PredicateObjectMap, stream: ElementStream): OpProjection =
    {
      return createProjection(t, tMap, nMap, poMap, stream, null);
    }

  private def createProjection(t: Triple, tMap: TriplesMap, nMap: TermMap, poMap: PredicateObjectMap, stream: ElementStream, sel: OpSelection): OpProjection =
    {
      //logger.debug("Creating projection, triple: "+t.toString());
      val unary = createRelation(tMap, nMap, stream)
      val nMapUri = tMap.uri
      //OpProjection projection = new OpProjection(nMapUri.substring(nMapUri.indexOf('#')), unary);
      var id = tMap.subjectMap.template
      if (id == null && tMap.subjectMap.constant != null)
        id = tMap.subjectMap.constant.toString
      if (id == null && tMap.subjectMap.column != null)
        id = tMap.subjectMap.column

      val projection = new OpProjection(id, unary);
      projection.triple = t;

      var constant: String = null;
      if (nMap!=null && nMap.constant != null) {
        constant = nMap.constant.toString
        val exp = new ValueXpr(constant);
        val op = new OperationXpr("constant", exp);
        if (t.getPredicate().getURI().equals(RDF.typeProp.getURI())) {
          projection.addExpression(t.getSubject().getName(), op);
        } else if (t.getObject().isVariable()) {
          projection.addExpression(t.getObject().getName(), op);

        } else if (sel != null) {
          sel.getExpressions.foreach { xpr =>
            val cond = xpr.asInstanceOf[(BinaryXpr)];
            projection.addExpression(cond.getLeft().toString(), op);
          }
        }
      } else if (nMap!=null &&nMap.column != null) {
        val varExp = new VarXpr(nMap.column)
        /*if (nMap.getColumnOperation()!=null)
			{varExp.setModifier(nMap.getColumnOperation());	}*/
        if (t.getObject().isVariable())
          projection.addExpression(t.getObject().getName(), varExp);
        else if (t.getPredicate().getURI().equals(RDF.typeProp.getURI()) && t.getSubject().isVariable())
          projection.addExpression(t.getSubject().getName(), varExp);

      } else if (nMap!=null && nMap.template != null) {
        logger.debug("Template " + nMap.template);
        val varExp = new VarXpr(extractColumn(nMap));
        varExp.setModifier(nMap.template);

        if (t.getObject().isVariable())
          projection.addExpression(t.getObject().getName(), varExp);
        else if (t.getPredicate().getURI().equals(RDF.typeProp.getURI()) && t.getSubject().isVariable())
          projection.addExpression(t.getSubject().getName(), varExp);

      } else if (poMap != null && poMap.refObjectMap != null) {
        //val refPO = nMap.asInstanceOf[RefPredicateObjectMap];
        val subject = reader.triplesMaps(poMap.refObjectMap.parentTriplesMap).subjectMap
        if (subject.column != null) {
          val varExp = new VarXpr(subject.column);
          /*if (subject.getColumnOperation()!=null)
					varExp.setModifier(subject.getColumnOperation());*/
          projection.addExpression(t.getObject().getName(), varExp);
        } else if (subject.template != null) {
          logger.debug("Template " + subject.template)
          val varExp = new VarXpr(extractColumn(subject));
          varExp.setModifier(subject.template);
          projection.addExpression(t.getObject().getName(), varExp);

        } else if (subject.constant != null) {
          val valu = new ValueXpr(subject.constant.asResource().getURI());
          val constOprXpr = new OperationXpr("constant", valu);
          projection.addExpression(t.getObject().getName(), constOprXpr);
        }

      }

      //We add the subject info to the projection here
      if (poMap != null) {
        //val poMap = nMap.asInstanceOf[PredicateObjectMap];

        val col = extractColumn(tMap.subjectMap);
        if (col != null) {
          val exp = new VarXpr(col);
          if (tMap.subjectMap.template != null)
            exp.setModifier(tMap.subjectMap.template)
          projection.addExpression(t.getSubject().getName(), exp);
        } else if (tMap.subjectMap.constant != null) {
          val opXpr = new OperationXpr("constant",
            new ValueXpr(tMap.subjectMap.constant.toString));
          projection.addExpression(t.getSubject().getName(), opXpr);
          //projection.link = poMap.getTriplesMap().getSubjectMap().getConstant().toString();
        }
        if (poMap.objectMap != null && poMap.objectMap.constant != null) {
          projection.link = poMap.objectMap.constant.toString();
        } else if (t.getObject().isVariable()) {
          projection.link = t.getObject().getName() + projection.getId();
        }
      }
      if (poMap!=null &&poMap.refObjectMap != null) {
        //val refPoMap = nMap.asInstanceOf[RefPredicateObjectMap];
        val parentMapUri = poMap.refObjectMap.parentTriplesMap
        projection.link = parentMapUri.substring(parentMapUri.indexOf('#'));
        logger.debug("link" + projection.link);
      }

      //logger.debug("Created projection: "+projection.toString());
      return projection;
    }

  private def extractColumn(sMap: TermMap): String =
    {
      if (sMap.column != null)
        return sMap.column
      else if (sMap.template != null) {
        val template = sMap.template
        val i = template.indexOf('{') + 1;
        val f = template.indexOf('}');
        if (i > 0)
          return template.substring(i, f);
        else
          return template;

      } else
        return null;
    }



  private def createRelation(tMap: TriplesMap, nMap: TermMap, stream: ElementStream): OpUnary =
    {
      var tableid = "";
      var extentName = "";
     
      val uri=new URI(tMap.uri).getFragment()
    
      logger.debug("Creating relation: " + nMap +
        " table: " + tMap.logicalTable.tableName);
      if (nMap!=null && nMap.constant != null) {
        tableid = nMap.constant.toString();
        extentName = tMap.logicalTable.tableName
      } else if (tMap.logicalTable.tableName != null) {
        tableid = tMap.logicalTable.tableName
        extentName = tableid;
      } else if (tMap.logicalTable.sqlQuery != null) {
        tableid = SQLParser.tableAlias(tMap.logicalTable.sqlQuery)

        //extentName = "(" + tMap.logicalTable.sqlQuery + ") " + tableid;
        extentName=tableid
      }
    
      
      
      val relation =
        if (stream != null) {
          logger.debug("Create window: " + stream.getUri());
          val window = new OpWindow(tableid+uri, null);
          val sw = stream.getWindow().asInstanceOf[ElementTimeWindow];
          if (sw != null) {
            val win = new Window();
            win.setFromOffset(sw.getFrom().getOffset());
            win.setFromUnit(sw.getFrom().getUnit());
            if (sw.getTo() != null) {
              win.setToOffset(sw.getTo().getOffset());
              win.setToUnit(sw.getTo().getUnit());
            }
            if (sw.getSlide() != null) {
              win.setSlide(sw.getSlide().getTime());
              win.setSlideUnit(sw.getSlide().getUnit());
            }
            window.setWindowSpec(win);
          }
          window;
        } else {
          new OpRelation(tableid+"lala");
        }
      
      relation.setExtentName(extentName);
      /*did I ever use the unique index?
      if (nMap.getTriplesMap().getTableUniqueIndex() != null)
        relation.getUniqueIndexes().add(nMap.getTriplesMap().getTableUniqueIndex());
*/
      logger.debug("Created relation: " + relation.getExtentName());

      if (tMap.logicalTable.sqlQuery!=null){
        val cond=SQLParser.selections(tMap.logicalTable.sqlQuery)
        if (cond.isDefined){
          val sel=createSelection(cond.get)
          sel.setSubOp(relation)
          sel
        }
        else relation
      }        
      else
        relation
    }
    

}