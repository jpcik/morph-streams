package es.upm.fi.oeg.integration.adapter.pachube;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import es.upm.fi.dia.oeg.integration.QueryBase;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpRoot;
import es.upm.fi.dia.oeg.integration.algebra.OpUnion;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.oeg.integration.adapter.pachube.model.Datastream;
import es.upm.fi.oeg.integration.adapter.pachube.model.Environment;

public class PachubeQuery extends QueryBase implements SourceQuery
{
	private Collection<Environment> environments;
	private Map<String,Map<String,String>> projectionAlias;

	public PachubeQuery()
	{
		projectionAlias = Maps.newHashMap();
	}

	public Collection<Environment> getEnvironments() {
		return environments;
	}

	@Override
	public void load(OpInterface op)
	{
		super.load(op);
		environments = Lists.newArrayList();
		build(op);
	}
	
	@Override
	public Map<String, Attribute> getProjectionMap()
	{
		Map<String,Attribute> projectionMap = new HashMap<String,Attribute>();
		Environment e = environments.iterator().next();
		for (Datastream d:e.getDatastreams())
		{
			projectionMap.put(d.getAlias(), null);
		}
		projectionMap.put(e.getTimeAlias(),null);
		return projectionMap;	
	}

	private void build(OpInterface op)
	{
		if (op instanceof OpRoot)
		{
			build(((OpRoot) op).getSubOp());			
		}
		else if (op instanceof OpMultiUnion)
		{
			OpMultiUnion union = (OpMultiUnion)op;
			for (OpInterface child:union.getChildren().values())
			{
				build (child);
			}
		}
		else if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			if (proj.getSubOp() instanceof OpRelation)
			{
				Environment e = new Environment();
				e.setId(proj.getRelation().getExtentName());
				for (Entry<String,String> entry:proj.getVarMappings().entrySet())
				{
					if (!entry.getValue().equals("at"))
					{
					Datastream ds = new Datastream();
					ds.setAlias(entry.getKey());
					ds.setId(entry.getValue());
					e.getDatastreams().add(ds);
					}
					else
					e.setTimeAlias(entry.getKey());
				}
				environments.add(e );
			}
			else
				build(proj.getSubOp());
		}
	}
	
	
	@Override
	public String serializeQuery()
	{
		String call = "";
		for (Environment e:environments)
		{
			call+=e.getId()+" ";
		}
		return call;
	
	}

	@Override
	public void setOriginalQuery(String sparqlQuery) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getOriginalQuery() {
		// TODO Auto-generated method stub
		return null;
	}
}
