package es.upm.fi.dia.oeg.integration;

import java.util.HashMap;
import java.util.Map;

import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpJoin;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpRoot;
import es.upm.fi.dia.oeg.integration.algebra.OpUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpWindow;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;

public abstract class QueryBase implements SourceQuery
{

	Map<String,String> constants = new HashMap<String,String>();
	Map<String,String> modifiers = new HashMap<String,String>();
	Map<String, String> staticConstants = new HashMap<String,String>();

	
	@Override
	public Map<String, String> getConstants()
	{
		
		return constants;
	}

	@Override
	public Map<String, String> getModifiers()
	{
		return modifiers;
	}
	
	@Override
	public Map<String,String> getStaticConstants()
	{
		return staticConstants;
	}

	@Override
	public Map<String, Attribute> getProjectionMap()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void load(OpInterface op)
	{
		if (op == null)
		{	//return "";
			
		}
		if (op instanceof OpRoot)
		{
			//yield(op);
			load(((OpRoot) op).getSubOp());
		}
		else if (op instanceof OpUnion)
		{
			OpUnion union = (OpUnion)op;
			load(union.getLeft()); 
			load(union.getRight());
			//yield(union);
		}
		
		else if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			//String select = "";
			String extent = proj.getRelation().getExtentName();
			int pos =0;
			
			for (Map.Entry<String, Xpr> entry : proj.getExpressions().entrySet())
			{
				//select += entry.getValue().toString()+ " AS "+entry.getKey();
				String lowerkey = entry.getKey().toLowerCase();
				if (entry.getValue() instanceof OperationXpr)
				{
					OperationXpr opXpr = (OperationXpr)entry.getValue();
					if (opXpr.getOp().equals("postproc"))
						constants.put(entry.getKey().toLowerCase(), opXpr.getParam().toString());
					else if (opXpr.getOp().equals("constant"))
						staticConstants .put(lowerkey+extent, opXpr.getParam().toString());
					
				}
				if (entry.getValue() instanceof VarXpr)
				{
					VarXpr var = (VarXpr)entry.getValue(); 
					if (var.getModifier()!=null)
					{					
						modifiers.put(lowerkey+extent, var.getModifier());
					}
				}
				//if (pos < proj.getExpressions().size()-1) select += ", ";
				pos++;
			}
			load(proj.getSubOp());
			//yield(proj);
			//return "(SELECT "+ select+" FROM "+build(proj.getSubOp())+")";
		}
		else if (op instanceof OpWindow)
		{
			//OpWindow win = (OpWindow)op;
			//return win.getExtentName()+ serializeWindowSpec(win.getWindowSpec());
		}
		else if (op instanceof OpRelation)
		{
			//OpRelation rel = (OpRelation)op;
			//return rel.getExtentName();
		}
		//else if (op.getName().equals("join"))
		else if (op instanceof OpMultiUnion)
		{
			OpMultiUnion union = (OpMultiUnion)op;
			for (OpInterface child:union.getChildren().values())
			{
				load(child);
			}
		}
		else if (op instanceof OpJoin)
		{
			OpJoin join = (OpJoin)op;
			load(join.getLeft());
			load(join.getRight());
		}
		else 
		{
			
			//return "";
		}

	}


	

	@Override
	public String serializeQuery()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
