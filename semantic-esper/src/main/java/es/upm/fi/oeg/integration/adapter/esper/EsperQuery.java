package es.upm.fi.oeg.integration.adapter.esper;


import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEqlQuery;
import es.upm.fi.dia.oeg.integration.algebra.OpBinary;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpJoin;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpRoot;
import es.upm.fi.dia.oeg.integration.algebra.OpSelection;
import es.upm.fi.dia.oeg.integration.algebra.OpWindow;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;

public class EsperQuery extends SNEEqlQuery implements SourceQuery
{
	String originalQuery;
	Map<String,Attribute> projectionMap = new HashMap<String,Attribute>();
	
	@Override
	public void load(OpInterface op)
	{
		super.load(op);
		this.innerQuery = build(op);
	}
	
	@Override
	public Map<String, Attribute> getProjectionMap()
	{
		return projectionMap;
		/*
		Map<String,String> alias = this.projectionAlias.entrySet().iterator().next().getValue();
		for (String key:alias.keySet())
		{
			projectionMap.put(key, null);
		}
		return projectionMap;*/	
	}
	
	@Override
	public void setOriginalQuery(String sparqlQuery) {
		originalQuery = sparqlQuery;
		
	}
	@Override
	public String getOriginalQuery()
	{
		return originalQuery;
	}
	
	/*
	@Override
	public String serializeQuery()
	{
		//return alternativeSerialization();
		return innerQuery;
	}*/
	
	private String build(OpInterface op)
	{
		if (op == null)
			return "";
		if (op instanceof OpRoot)
		{
			return build(((OpRoot) op).getSubOp())+";";
		}
		else if (op.getName().equals("union"))
		{
			OpBinary union = (OpBinary)op;
			return build(union.getLeft()) + " UNION  " + build(union.getRight());			
		}
		else if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			String select = serializeSelect(proj);//"time AS instant, windspeed AS windspeed";//
			for (String key:proj.getVarMappings().keySet())
			{
				projectionMap.put(key, null);
			}
			return "SELECT "+ select+" FROM "+build(proj.getSubOp())+"";
		}
		else if (op instanceof OpWindow)
		{
			OpWindow win = (OpWindow)op;
			return win.getExtentName();//+ serializeWindowSpec(win.getWindowSpec())+ " "+win.getExtentName();
		}
		else if (op instanceof OpRelation)
		{
			OpRelation rel = (OpRelation)op;
			return rel.getExtentName();
		}
		else if (op instanceof OpSelection)
		{
			OpSelection sel = (OpSelection)op;
			return build(sel.getSubOp())+ " WHERE "+serializeExpressions(sel.getExpressions(),null); 
		}
		else if (op instanceof OpJoin)
		{
			OpJoin join = (OpJoin)op;
			OpProjection opLeft = null;
			OpProjection opRight = null;
			if (join.getLeft() instanceof OpProjection && 
					join.getRight() instanceof OpProjection)
			{
				opLeft = (OpProjection)join.getLeft();
				opRight = (OpProjection)join.getRight();				
			}
			else if (join.getLeft() instanceof OpMultiUnion &&
					join.getRight() instanceof OpMultiUnion &&
				((OpMultiUnion)join.getLeft()).getChildren().size()==1)
			{
				OpMultiUnion uLeft = (OpMultiUnion)join.getLeft();
				OpMultiUnion uRight = (OpMultiUnion)join.getRight();	
				opLeft = (OpProjection)uLeft.getChildren().values().iterator().next();
				opRight = (OpProjection)uRight.getChildren().values().iterator().next();
			}
			else
				throw new NotImplementedException("Nested join queries not supported in target language");
			String select = "SELECT RSTREAM "+serializeSelect(opLeft,"",true)+ ", "+serializeSelect(opRight,"2",true) +
				" FROM "+build(opLeft.getSubOp())+","+build(opRight.getSubOp());
			if (!join.getConditions().isEmpty())
			{			
				//System.err.println(unAlias("sada",join));
				select+=" WHERE "+serializeExpressions(join);
			}
			return "("+select+")";
		}
		else if (op instanceof OpMultiUnion)
		{
			OpMultiUnion union = (OpMultiUnion)op;
			String unionString = "";
			for (OpInterface col:union.getChildren().values())
			{
			//for (OpInterface child :col)
			{
				unionString+=build(col)+ " UNION ";
			}
			}
			return unionString.substring(0,unionString.length()-6);
		}
		//else if (op.getName().equals("join"))
		else return "";
	}

}
