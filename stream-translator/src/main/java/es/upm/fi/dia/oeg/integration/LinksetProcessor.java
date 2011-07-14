package es.upm.fi.dia.oeg.integration;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.DataSource;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.resultset.ResultSetException;
//import com.ontotext.jena.SesameDataset;

import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueSetXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;

public class LinksetProcessor
{

	private static Logger logger = Logger.getLogger(LinksetProcessor.class.getName());

	RepositoryConnection connection;
	Dataset linksDataset;
	
	public LinksetProcessor(String linksFile) throws IntegratorConfigurationException
	{
		logger.debug("Reading the links file: "+linksFile);
		InputStream is = LinksetProcessor.class.getClassLoader().getResourceAsStream(linksFile);

		
		Model m = ModelFactory.createDefaultModel();
		RDFReader reader =m.getReader("TURTLE");
		reader.read(m, is, "http://example.com#");
		linksDataset = DatasetFactory.create(m);
		
		/*
		MemoryStore st = new MemoryStore();
		try
		{
			st.initialize();
		} catch (SailException e)
		{
			throw new IntegratorConfigurationException("Cannot init linkset", e);
		}
		Repository rr = new SailRepository(st);
		try
		{
			connection = rr.getConnection();
			connection.add(is , "http://coco.com/papa#", RDFFormat.TURTLE, new Resource[]{});
		} catch (RepositoryException e)
		{
			throw new IntegratorConfigurationException("Unable to load linksets", e);
		} catch (RDFParseException e)
		{
			throw new IntegratorConfigurationException("Invalid linkset file", e);
		} catch (IOException e)
		{
			throw new IntegratorConfigurationException("Unable to load linksets", e);
		}*/
		
	}
	
public 	OpProjection findPredicate(Triple t, String mappinguri) throws QueryException 
{
	logger.debug("Finding predicate: "+t+" in mapping "+mappinguri);
	//SesameDataset ds = new SesameDataset(connection);
	//Model m = Mo ds.getDefaultGraph();
	

	String queryString ="PREFIX void: <http://rdfs.org/ns/void#> " +
						" SELECT ?target ?dump ?sparql " +
						" WHERE { \n" +
						//" ?links  void:subjectsTarget <"+mappinguri+">; \n"+
						" ?links  void:objectsTarget ?target; \n"+
						"         void:linkPredicate <"+t.getPredicate().getURI()+">. \n"+
						" ?target void:dataDump ?dump; \n"+
						"         void:sparqlEndpoint ?sparql. \n"+
						"} ";
		
	logger.debug("Executing query: "+queryString);
	com.hp.hpl.jena.query.Query query = QueryFactory.create(queryString) ;
	QueryExecution qexec = QueryExecutionFactory.create(query, this.linksDataset);
	
	String dumpUri = null;
	Resource sparqlEndpoint = null;
	ResultSet rs = qexec.execSelect();
	while (rs.hasNext())
	{
		QuerySolution qs = rs.nextSolution();
		qs.getResource("target");
		dumpUri = qs.getResource("dump").getURI();
		sparqlEndpoint = qs.getResource("sparql");
		
	}			
	
	queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+ 
				"SELECT ?var "+
				"WHERE { \n" +
					"?var <"+t.getPredicate().getURI()+"> <"+t.getObject().getURI()+"> " +
				"}\n";
	query = QueryFactory.create(queryString) ;
	
	if (sparqlEndpoint==null)
	{
		Model m = ModelFactory.createDefaultModel();
		RDFReader reader =m.getReader("TURTLE");
		dumpUri = dumpUri.substring(8);
		
		logger.debug("Querying metadata source: "+dumpUri);
		
		InputStream is = LinksetProcessor.class.getClassLoader().getResourceAsStream(dumpUri);
		
		reader.read(m, is, "");
		
		qexec = QueryExecutionFactory.create(query, m);
	}
	else
	{
		logger.debug("metadata quey: "+query);
		qexec = QueryExecutionFactory.sparqlService(sparqlEndpoint.getURI(), query);
	}
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
		p.addExpression(t.getSubject().getName(), vs);
		return p;
	}
	
	while (res.hasNext())
	{
		QuerySolution qs = res.next();
		p.addBinding(t.getSubject().getName(), qs.getResource("var").getURI());
		vs.getValueSet().add(qs.getResource("var").getURI());
	}
	p.addExpression(t.getSubject().getName(), vs);
	return p;
	/*
		

	
Model model = ModelFactory.createDefaultModel();
RDFReader arp = model.getReader("TURTLE");
arp.read(model,"file:///g:/ssg4env/integrator-repository/workspace/ssg/swissex-metadata.n3");
Query q = QueryFactory.create(qs ) ;
QueryExecution qexec = QueryExecutionFactory.create(q, model) ;
ResultSet results = qexec.execSelect() ;
for ( ; results.hasNext() ; )
{
QuerySolution soln = results.nextSolution() ;
String value = soln.getResource("var").getURI();
//logger.debug("value: "+value);
/*
OpRelation rel = new OpRelation(t.getSubject().getName());
rel.setExtentName("constants");
OpProjection p = new OpProjection(t.getSubject().getName(), rel);
OperationXpr constXpr = new OperationXpr("constant", new ValueXpr(value));
p.addExpression(t.getSubject().getName(), constXpr);*/
//pro = p;
//if (opCurrent!=null) opCurrent = union(opCurrent,pro);
//else opCurrent = pro;
//}
//return results;

}
}
