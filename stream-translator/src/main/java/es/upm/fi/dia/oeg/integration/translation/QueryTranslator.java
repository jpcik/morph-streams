package es.upm.fi.dia.oeg.integration.translation;

import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.resultset.ResultSetException;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.hp.hpl.jena.sparql.syntax.TemplateGroup;
import com.hp.hpl.jena.sparql.syntax.TemplateTriple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.integration.adapter.gsn.GsnUrlQuery;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEqlQuery;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpJoin;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpRoot;
import es.upm.fi.dia.oeg.integration.algebra.OpSelection;
import es.upm.fi.dia.oeg.integration.algebra.OpUnary;
import es.upm.fi.dia.oeg.integration.algebra.OpWindow;
import es.upm.fi.dia.oeg.integration.algebra.Window;
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueSetXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.dia.oeg.sparqlstream.StreamQuery;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementStream;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementTimeWindow;
import es.upm.fi.dia.oeg.integration.LinksetProcessor;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.QueryExecutor;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.morph.r2rml.NodeMap;
import es.upm.fi.dia.oeg.morph.r2rml.PredicateObjectMap;
import es.upm.fi.dia.oeg.morph.r2rml.R2RModel;
import es.upm.fi.dia.oeg.morph.r2rml.RefPredicateObjectMap;
import es.upm.fi.dia.oeg.morph.r2rml.SubjectMap;
import es.upm.fi.dia.oeg.morph.r2rml.TriplesMap;



public class QueryTranslator 
{
	private static Logger logger = Logger.getLogger(QueryTranslator.class.getName());
	private Properties props;
	
	private R2RModel r2r;
	private LinksetProcessor linker;
	private boolean metadataMappings = false;
	private OpProjection bindings;
	
	public QueryTranslator(Properties props)
	{
		this.props = props;
		r2r = new R2RModel();		
		//if (props.getProperty(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED).equals("true"))
		//	metadataMappings = true;
	}
	
	public QueryTranslator(Properties props, String mappingEndopint)
	{
		this.props = props;
		r2r = new R2RModel(mappingEndopint);		
		//if (props.getProperty(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED).equals("true"))
		//	metadataMappings = true;
	}
	/*
	public void getConstruct(String queryString)
	{
		StreamQuery query = (StreamQuery)StreamQueryFactory.create(queryString);	
	}*/
	
	public Map<String,Attribute> getProjectList(String queryString)
	{
		HashMap<String,Attribute> map = new HashMap<String, Attribute>();
		StreamQuery query = (StreamQuery)StreamQueryFactory.create(queryString);
		for (String var :query.getResultVars())
		{
			map.put(var.toLowerCase(), null);
		}
		return map;
		
	}
	
	public SourceQuery translate(String queryString, URI mappingUri) throws  QueryTranslationException 
	{
		
		OpInterface opNew = null;
		opNew = translateToAlgebra(queryString, mappingUri);
		return transform(opNew);
	}

	public SourceQuery transform(OpInterface algebra)
	{
		SourceQuery resquery = null;
		if (props.getProperty(QueryExecutor.QUERY_EXECUTOR_ADAPTER).equals("gsn"))
			resquery = new GsnUrlQuery();
		else
			resquery = new SNEEqlQuery();	
		resquery.load(algebra);
		logger.info(resquery.serializeQuery());
		
		return resquery; 
		
	}

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
	
	public OpInterface translateToAlgebra(String queryString, URI mappingUri) throws  QueryTranslationException
	{
		long ini = System.currentTimeMillis();
		StreamQuery query = (StreamQuery)StreamQueryFactory.create(queryString);
		Op op = Algebra.compile(query);
		long span1 = System.currentTimeMillis()-ini;
		if (mappingUri!=null)
		{
			try
			{
				//if (r2r.getTriplesMap()==null)
					r2r.read(mappingUri);
			} catch (InvalidR2RDocumentException e)
			{
				throw new QueryTranslationException(e);
			} catch (InvalidR2RLocationException e)
			{
				throw new QueryTranslationException(e);
			}
		}
		//linker = new LinksetProcessor(mappingUri.toString());
		long span2 = System.currentTimeMillis()-ini;
		
		OpProjection binds = (OpProjection)partition(op);
		this.bindings = binds;
		OpInterface opo= navigate(op,query);
		if (binds !=null)
		{
		OpProjection mainPro = (OpProjection)opo;	
		  mainPro.setSubOp(opo.build(binds));
		}
		OpRoot opNew = new OpRoot(null);
		
		if (query.getConstructTemplate()!=null)
		{//TODO ugliest code ever, please refactor
			OpProjection cProj = new OpProjection("mainProjection", opo);
			TemplateGroup tg = (TemplateGroup)query.getConstructTemplate();
			for (Template temp:tg.getTemplates())
			{				
				String var="";
				TemplateTriple tt = (TemplateTriple)temp;
				if (tt.getTriple().getSubject().isVariable())
				{
					var = tt.getTriple().getSubject().getName();
					VarXpr exp = new VarXpr(var);
					cProj.addExpression(var,exp);
				}
				if (tt.getTriple().getObject().isVariable())
				{
					var = tt.getTriple().getObject().getName();
					VarXpr exp = new VarXpr(var);
					cProj.addExpression(var,exp);
				}

			}
				
			opNew.setSubOp(cProj);
		}
		
		else
		{
		opNew.build(opo);
		}
		long span3 = System.currentTimeMillis()-ini;

		opNew.display();


		QueryOptimizer opt = new QueryOptimizer();
		opt.staticOptimize(opNew);
		long span4 = System.currentTimeMillis()-ini;

		opNew.display();

		System.err.println(span1+"-"+span2+"-"+span3+"-"+span4);
		return opNew;
	}
	


	public OpInterface navigate(Op op, StreamQuery query ) 
	{
		if (op instanceof OpBGP)
		{
			OpBGP bgp = (OpBGP)op;						
			OpInterface opCurrent = null;
			OpInterface pra = null;
			List<Triple> triples = bgp.getPattern().getList();
			for (Triple t : triples)
			{
				if (t.getPredicate().getURI().equals(RDF.type.getURI()))
				{
					
					Collection<TriplesMap> tMaps = r2r.getTriplesMapForUri(t.getObject().getURI());
					if (tMaps.isEmpty()) continue;
					opCurrent = null;
					for (TriplesMap tMap:tMaps)
					{//TODO adapt for multiple graphs
						logger.debug("Mapping graphs for: "+tMap.getUri()+ " - "+tMap.getSubjectMap().getGraphSet());
						Set<String> graphs = tMap.getSubjectMap().getGraphSet();
						ElementStream stream = null;
						if (graphs!=null && graphs.size()>0)
							stream = query.getStream(tMap.getSubjectMap().getGraphSet().iterator().next());
						OpProjection projection = createProjection(t,tMap.getSubjectMap(),stream);
						
						if (opCurrent!=null) opCurrent = union(opCurrent,projection);
						else opCurrent = projection;
					}					
				}
				else 
				{
					Collection<PredicateObjectMap> poMaps = r2r.getPredicateObjectMapForUri(t.getPredicate().getURI());
					if (poMaps.isEmpty()) continue;
					OpInterface pro = null;
					opCurrent=null;
					if (poMaps.size()==-8)
					{
						logger.debug("predicate not found: "+t.getPredicate().getURI());
						try
						{
							pro = linker.findPredicate(t, null);
							if (opCurrent!=null) opCurrent = union(opCurrent,pro);
							else opCurrent = pro;
						} catch (QueryException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					for (PredicateObjectMap poMap:poMaps)																	
					{
						logger.debug("Graphs: "+poMap.getGraph());
						ElementStream stream = query.getStream(poMap.getGraph());
						
						if (t.getObject().isURI())
						{
							OpSelection selection = createSelection(t,poMap,  t.getObject().getURI());
								
							pro = createProjection(t, poMap, stream,selection);
							pro.build(selection);
							if (opCurrent!=null) opCurrent = union(opCurrent,pro);
							else opCurrent = pro;
							
						}
						else
						{
							pro = createProjection(t, poMap, stream);
							if (opCurrent!=null) opCurrent = union(opCurrent,pro);
							else opCurrent = pro;							
						}
						
					}
					
				}
				if(opCurrent==null)
				{
					return null;
				}
				if (pra!=null) pra = pra.build(opCurrent);
				else pra = opCurrent;
				
				
			}return pra;

		}
		else if (op instanceof OpProject)
		{
			OpProject project = (OpProject)op;
			OpProjection proj = new OpProjection("mainProjection", null);
			for (Var var : project.getVars())
			{
				VarXpr exp = new VarXpr(var.getVarName());
				proj.addExpression(var.getVarName(),exp);
			}
			OpInterface opo= navigate(project.getSubOp(),query);
			proj.setSubOp(opo);
			return proj;
		}
		
		else if (op instanceof com.hp.hpl.jena.sparql.algebra.op.OpJoin) 
		{
			com.hp.hpl.jena.sparql.algebra.op.OpJoin opJoin = (com.hp.hpl.jena.sparql.algebra.op.OpJoin)op;
			OpInterface l =navigate(opJoin.getLeft(),query);
			OpInterface r =navigate(opJoin.getRight(),query);
			OpJoin join = new OpJoin("whatHere", l, r);

			return join;
		}
		else if (op instanceof OpFilter)
		{
			OpFilter filter = (OpFilter)op;
			Iterator<Expr> it = filter.getExprs().iterator();
			OpSelection selection = new OpSelection("selec",null);
			while (it.hasNext())
			{
				ExprFunction2 expr = (ExprFunction2)it.next();
				ExprFunction function = expr.getFunction();
				BinaryXpr xpr = null;
				if (expr.getArg2().getConstant()!=null)
					xpr = BinaryXpr.createFilter(expr.getArg1().getVarName(), function.getOpName(), 
						expr.getArg2().getConstant().toString());
				else
					xpr = BinaryXpr.createFilter(new VarXpr(expr.getArg1().getVarName()), 
							function.getOpName(), 
							new VarXpr(expr.getArg2().getVarName()));
				selection.addExpression(xpr);
				//function.
				logger.debug("filter "+ selection);
			}
			//logger.debug("Filter "+filter.toString());
			
			OpInterface inner = navigate(filter.getSubOp(),query);
			selection.setSubOp(inner);
			return selection;
		}
		else 
		{
			logger.info("None of above: "+op.getClass().getName());
			throw new NotImplementedException("Query processing for SPARQL operation not supported: "+op.getClass().getName());
		}
		
	//return null;	
	}
	
	
	private OpInterface union(OpInterface left,OpInterface right)
	{
		//OpUnion union1 = new OpUnion(left, right);
		//if (true) return union1;
		
		OpMultiUnion union = null;
		if (left instanceof OpMultiUnion )
			union = (OpMultiUnion) left;
		else
		{
			union = new OpMultiUnion("multiunion");
			OpProjection proj = (OpProjection)left;

			union.getChildren().put(proj.getId(),proj);//+proj.getRelation().getExtentName(), proj);
			Multimap<String,String> map = HashMultimap.create();
			
			map.put(proj.getId(), proj.getId()+proj.getRelation().getExtentName());
			union.index.put(proj.triple.getSubject().getName(), map);
			if (proj.triple.getObject().isVariable() && proj.link!=null)
			{
				Multimap<String,String> map2 = HashMultimap.create();
				map2.put(proj.link, proj.getId()+proj.getRelation().getExtentName());
				union.index.put(proj.triple.getObject().getName(), map2);
			}
		}

		OpProjection proj = (OpProjection)right; 
		if (proj.getId()==null)
			logger.debug("is null"+ proj);
		union.getChildren().put(proj.getId(),proj);//+proj.getRelation().getExtentName(), proj);
		Multimap<String,String> map = union.index.get(proj.triple.getSubject().getName());
		if (map ==null)
		{	map = HashMultimap.create();				
			union.index.put(proj.triple.getSubject().getName(), map);
		}			
		map.put(proj.getId(), proj.getId()+proj.getRelation().getExtentName());
		
		if (proj.triple.getObject().isVariable() && proj.link!=null)
		{
			map = union.index.get(proj.triple.getObject().getName());
			if (map ==null)
			{
				map = HashMultimap.create();
				union.index.put(proj.triple.getObject().getName(), map);
			}
			map.put(proj.link, proj.getId()+proj.getRelation().getExtentName());
			logger.debug("index map"+proj.link+"--"+proj.getId());

		}
		return union;
	}
	
	
private OpSelection createSelection(Triple t,NodeMap nMap,String value)
{
	String var = nMap.getColumn()==null?"localVar"+t.getPredicate().getLocalName()+t.getSubject().getName():nMap.getColumn();
	OpSelection selection = new OpSelection(var, null);
	BinaryXpr xpr = BinaryXpr.createFilter(var, "=", value);
	
	selection.addExpression(xpr);
	
	return selection;
}	

	private OpProjection createProjection(Triple t, NodeMap nMap, ElementStream stream)
	{
		return createProjection(t, nMap, stream,null);
	}
	
	private OpProjection createProjection(Triple t, NodeMap nMap, ElementStream stream,OpSelection sel)
	{
		//logger.debug("Creating projection, triple: "+t.toString());
		OpUnary unary = createRelation(nMap,stream);		
		String nMapUri = nMap.getTriplesMap().getUri();
		//OpProjection projection = new OpProjection(nMapUri.substring(nMapUri.indexOf('#')), unary);
		String id = nMap.getTriplesMap().getSubjectMap().getTemplate();
		if (id==null && nMap.getTriplesMap().getSubjectMap().getSubject()!=null )
			id = nMap.getTriplesMap().getSubjectMap().getSubject().toString();
		if (id==null && nMap.getTriplesMap().getSubjectMap().getColumn()!=null )
			id = nMap.getTriplesMap().getSubjectMap().getColumn();
			
		OpProjection projection = new OpProjection(id, unary);
		projection.triple = t;	
		
		String constant = null;
		if (nMap.getConstant()!=null)
		{
			constant = nMap.getConstant().toString();
			ValueXpr exp = new ValueXpr(constant);
			OperationXpr op = new OperationXpr("constant",exp);
			if (t.getPredicate().getURI().equals(RDF.type.getURI()))
			{
				projection.addExpression(t.getSubject().getName(), op);
			}
			else if (t.getObject().isVariable())
			{	projection.addExpression(t.getObject().getName(), op);
			
			}
			else if (sel!=null)
			{
				for (Xpr xpr:sel.getExpressions())
				{
					BinaryXpr cond = (BinaryXpr)xpr;			
					projection.addExpression(cond.getLeft().toString(),op);
				}
			}
		}
		else if (nMap.getColumn() != null)
		{			
			VarXpr varExp = new VarXpr(nMap.getColumn());
			/*if (nMap.getColumnOperation()!=null)
			{varExp.setModifier(nMap.getColumnOperation());	}*/
			if (t.getObject().isVariable())
				projection.addExpression(t.getObject().getName(),varExp);
			else if (t.getPredicate().getURI().equals(RDF.type.getURI()) && t.getSubject().isVariable())
				projection.addExpression(t.getSubject().getName(),varExp);
	
		}
		else if (nMap.getTemplate()!=null)
		{
			logger.debug("Template "+nMap.getTemplate());
			VarXpr varExp = new VarXpr(extractColumn(nMap));
			varExp.setModifier(nMap.getTemplate());
		
			if (t.getObject().isVariable())
				projection.addExpression(t.getObject().getName(),varExp);
			else if (t.getPredicate().getURI().equals(RDF.type.getURI()) && t.getSubject().isVariable())
				projection.addExpression(t.getSubject().getName(),varExp);
			
		}

		else if (nMap instanceof RefPredicateObjectMap)
		{
			RefPredicateObjectMap refPO = (RefPredicateObjectMap)nMap;
			SubjectMap subject = refPO.getRefObjectMap().getParentTriplesMap().getSubjectMap();
			if (subject.getColumn()!=null)
			{
				VarXpr varExp = new VarXpr(subject.getColumn());			
				/*if (subject.getColumnOperation()!=null)
					varExp.setModifier(subject.getColumnOperation());*/
				projection.addExpression(t.getObject().getName(), varExp);
			}
			else if (subject.getTemplate()!=null)
			{
				logger.debug("Template "+subject.getTemplate());
				VarXpr varExp = new VarXpr(extractColumn(subject));
				varExp.setModifier(subject.getTemplate());
				projection.addExpression(t.getObject().getName(), varExp);
				
			}
			else if (subject.getSubject()!=null)
			{
				ValueXpr val = new ValueXpr(subject.getSubject().asResource().getURI());
				OperationXpr constOprXpr = new OperationXpr("constant", val); 
				projection.addExpression(t.getObject().getName(), constOprXpr);
			}
				
		}
		
		//We add the subject info to the projection here
		if (nMap instanceof PredicateObjectMap)
		{
			PredicateObjectMap poMap = (PredicateObjectMap)nMap;
			
			String col = extractColumn(poMap.getTriplesMap().getSubjectMap());
			if (col!=null)
			{
				VarXpr exp = new VarXpr(col);
				if (poMap.getTriplesMap().getSubjectMap().getTemplate()!=null)
					exp.setModifier(poMap.getTriplesMap().getSubjectMap().getTemplate());
				projection.addExpression(t.getSubject().getName(), exp);
			}
			else if (poMap.getTriplesMap().getSubjectMap().getConstant()!=null)
			{	
				OperationXpr opXpr = new OperationXpr("constant",
						new ValueXpr(poMap.getTriplesMap().getSubjectMap().getConstant().toString()));
				projection.addExpression(t.getSubject().getName(), opXpr);
				//projection.link = poMap.getTriplesMap().getSubjectMap().getConstant().toString();
			}
			if (poMap.getObjectMap()!=null && poMap.getObjectMap().getObject()!=null)
			{
				projection.link = poMap.getObjectMap().getObject().toString();
			}
			else if (t.getObject().isVariable())
			{
				projection.link = t.getObject().getName()+projection.getId();
			}
		}
		if (nMap instanceof RefPredicateObjectMap)
		{
			RefPredicateObjectMap refPoMap = (RefPredicateObjectMap) nMap;
			String parentMapUri=  refPoMap.getRefObjectMap().getParentTriplesMap().getUri();
			projection.link = parentMapUri.substring(parentMapUri.indexOf('#'));
			logger.debug("link"+projection.link);
		}
		
		//logger.debug("Created projection: "+projection.toString());
		return projection;
	}

	
	private String extractColumn(NodeMap sMap)
	{
		if (sMap.getColumn()!=null)
			return sMap.getColumn();
		else if (sMap.getTemplate()!=null)
		{
			String template = sMap.getTemplate();
			int i = template.indexOf('{')+1;
			int f = template.indexOf('}');
			if (i>0)
				return template.substring(i,f);
			else
				return template;
			
		}
		else
			return null;
	}

	private String getAlias(String query)
	{
		CCJSqlParserManager p = new CCJSqlParserManager();
		try
		{
			Statement s = p.parse(new StringReader(query));
			Select select = (Select)s;
			PlainSelect ps = (PlainSelect) select.getSelectBody();
			net.sf.jsqlparser.schema.Table tab = (net.sf.jsqlparser.schema.Table)ps.getFromItem();
			tab.getName();
		
			return tab.getName();
		} catch (JSQLParserException e)
		{
			//TODO do something smarter
			e.printStackTrace();
			return "";
		}
		
	}
	
	private OpUnary createRelation(NodeMap nMap, ElementStream stream)
	{
		String tableid = "";
		String extentName ="";		
		logger.debug("Creating relation: "+ nMap.getColumn()+ 
				" const: "+nMap.getConstant()+" table: "+nMap.getTriplesMap().getTableName());
		if (nMap.getConstant() != null)
		{
			tableid = nMap.getConstant().toString();
			extentName = nMap.getTriplesMap().getTableName();
		}
		else if (nMap.getTriplesMap().getTableName() != null)			
		{
			tableid = nMap.getTriplesMap().getTableName();
			extentName = tableid;
		}
		else if (nMap.getTriplesMap().getSqlQuery() !=null)
		{
			tableid= getAlias(nMap.getTriplesMap().getSqlQuery());
			
			extentName = "("+nMap.getTriplesMap().getSqlQuery()+") "+tableid;
		}
		OpRelation relation = null;
		if (stream!=null)
		{
			logger.debug("Create window: "+stream.getUri());
			OpWindow window = new OpWindow(tableid,null);
			ElementTimeWindow sw = (ElementTimeWindow)stream.getWindow();
			if (sw!=null)
			{
				Window win = new Window();
				win.setFromOffset(sw.getFrom().getOffset());
				win.setFromUnit(sw.getFrom().getUnit());
				if (sw.getTo()!=null)
				{
					win.setToOffset(sw.getTo().getOffset());
					win.setToUnit(sw.getTo().getUnit());
				}
				if (sw.getSlide()!=null)
				{
					win.setSlide(sw.getSlide().getTime());
					win.setSlideUnit(sw.getSlide().getUnit());
				}
				window.setWindowSpec(win);
			}
			relation= window;
		}
		else 
		{
			relation = new OpRelation(tableid);
		}
		relation.setExtentName(extentName);

		if (nMap.getTriplesMap().getTableUniqueIndex()!=null)
			relation.getUniqueIndexes().add(nMap.getTriplesMap().getTableUniqueIndex());
		
		logger.debug("Created relation: "+relation.getExtentName());
		
		return relation;

	}



	
	

	
	
	

}
