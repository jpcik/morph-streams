package es.upm.fi.dia.oeg.integration.adapter.gsn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.ListUtils;

import com.google.common.collect.Lists;

import es.upm.fi.dia.oeg.integration.QueryBase;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.algebra.OpBinary;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpRoot;
import es.upm.fi.dia.oeg.integration.algebra.OpSelection;
import es.upm.fi.dia.oeg.integration.algebra.OpUnary;
import es.upm.fi.dia.oeg.integration.algebra.OpUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpWindow;
import es.upm.fi.dia.oeg.integration.algebra.Window;
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_FieldSelector;
import gsn.webservice.standard.GSNWebServiceStub.StandardCriterion;

public class GsnQuery extends QueryBase implements SourceQuery
{

	private OpInterface operator;
	private Map<String,Map<String,String>> projectionAlias;
	private Window window;

	public GsnQuery()
	{
		projectionAlias = new HashMap<String,Map<String,String>>();
	}
	
	@Override
	public Map<String, Attribute> getProjectionMap()
	{
		Map<String,Attribute> projectionMap = new HashMap<String,Attribute>();
		Map<String,String> alias = this.projectionAlias.entrySet().iterator().next().getValue();
		for (String key:alias.keySet())
		{
			projectionMap.put(key, null);
		}
		return projectionMap;	
	}

	@Override
	public void load(OpInterface op)
	{
		super.load(op);
		this.operator = op;
		getWindow(operator);
		
	}

	@Override
	public String serializeQuery()
	{
		String sel ="\nSelectors: \n";
		for (GSNWebService_FieldSelector fs:getSelectors())
		{
			sel += fs.getVsname()+" {";
			for (String f:fs.getFieldNames())
				sel+=f+" ";
			sel+="}\n";
		}
		
		sel+="Conditions \n";
		for (StandardCriterion crit:getConditions())
		{
			sel+=crit.getField()+" "+crit.getOperator()+" "+crit.getValue()+"\n";
		}
		
		
		
		return sel;
	}

	public GSNWebService_FieldSelector[] getSelectors()
	{
		return getSelectors(operator).toArray(new GSNWebService_FieldSelector[0]);
	}
	
	private Collection<GSNWebService_FieldSelector> getSelectors(OpInterface op)
	{
		if (op == null)
			return new ArrayList<GSNWebService_FieldSelector>(); 
		if (op instanceof OpRoot)
		{
			return getSelectors(((OpRoot) op).getSubOp());
		}
		else if (op instanceof OpUnion)
		{
			OpBinary union = (OpBinary)op;
			Collection<GSNWebService_FieldSelector> left = getSelectors(union.getLeft());
			left.addAll(getSelectors(union.getRight()));
			return left;			
		}
		else if (op instanceof OpProjection)
		{
			ArrayList<GSNWebService_FieldSelector> selectors = new ArrayList<GSNWebService_FieldSelector>();
			GSNWebService_FieldSelector selector = new GSNWebService_FieldSelector();
			
			OpProjection proj = (OpProjection)op;
			OpRelation rel = proj.getRelation();
			if (rel == null)
				return selectors;
			selector.setVsname(rel.getExtentName());
			Map<String,String> alias = new HashMap<String,String>();
			
			for (Map.Entry<String, Xpr> entry : proj.getExpressions().entrySet())
			{
				if (entry.getValue() instanceof OperationXpr)
					continue;
				selector.addFieldNames(entry.getValue().toString());
				alias.put(entry.getKey().toLowerCase(),entry.getValue().toString() );
			}
			projectionAlias.put(selector.getVsname(), alias);
			selectors.add(selector);
			return selectors;
		}
		else if (op instanceof OpWindow)
		{
			return null;
		}
		else if (op instanceof OpRelation)
		{
			return null;
		}
		else if (op instanceof OpSelection)
		{
			return getSelectors(((OpSelection)op).getSubOp());
		}
		else if (op instanceof OpMultiUnion)
		{
			Collection<GSNWebService_FieldSelector> unionFields = Lists.newArrayList();
			OpMultiUnion union = (OpMultiUnion)op;
			for (OpInterface opi:union.getChildren().values())
			{
				unionFields.addAll(getSelectors(opi));
			}
			return unionFields;
		}
		//else if (op.getName().equals("join"))
		else return null;

	}
	
	private Collection<StandardCriterion> getConditions(OpInterface op)
	{
		if (op == null)
			return new ArrayList<StandardCriterion>(); 
		if (op instanceof OpRoot)
		{
			return getConditions(((OpRoot) op).getSubOp());
		}
		else if (op instanceof OpUnion)
		{
			OpBinary union = (OpBinary)op;
			Collection<StandardCriterion> left = getConditions(union.getLeft());
			//if (left ==null)
			//	left = new ArrayList<StandardCriterion>();
			left.addAll(getConditions(union.getRight()));
			return left;			
		}
		else if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			return getConditions(proj.getSubOp());
		}
		else if (op instanceof OpWindow)
		{
			return new ArrayList<StandardCriterion>();
		}
		else if (op instanceof OpRelation)
		{
			return new ArrayList<StandardCriterion>();
		}
		else if (op instanceof OpSelection)
		{
			OpSelection sel = (OpSelection)op;
		
			ArrayList<StandardCriterion> list = new ArrayList<StandardCriterion>();
			if (sel.getRelation()==null)
				return list;
			for (Xpr xpr:sel.getExpressions())
			{
			StandardCriterion crit = new StandardCriterion();
			crit.setCritJoin("AND");
			crit.setNegation("");
			BinaryXpr bin = (BinaryXpr)xpr;
			VarXpr var = (VarXpr)bin.getLeft();
			
			crit.setVsname(sel.getRelation().getExtentName());
			crit.setField(var.getVarName());
			//crit.setField(unAlias(crit.getVsname(), var.getVarName()));
			crit.setOperator(transformOperation(bin.getOp()));
			crit.setValue(bin.getRight().toString());
			list.add(crit);
			}
			return list;
		}
		else if (op instanceof OpMultiUnion) 
		{
			OpMultiUnion union = (OpMultiUnion)op;
			Collection<StandardCriterion> conds = Lists.newArrayList();
			for (OpInterface child:union.getChildren().values())
			{
				conds.addAll(getConditions(child));
			}
			return conds;
		}
		//else if (op.getName().equals("join"))
		else return null;

	}
	
	private String transformOperation(String op)
	{
		if (op.equals("<"))
			return "le";
		else if (op.equals(">"))
			return "ge";
		else if (op.equals("="))
			return "eq";
		else return null;
	}
	
	private void getWindow(OpInterface op)
	{
		if (op instanceof OpWindow)
		{
			OpWindow win = (OpWindow)op;
			
			if (window != null)
			{
				if (window.getFromOffset()<win.getWindowSpec().getFromOffset())
					window.setFromOffset(win.getWindowSpec().getFromOffset());
				if (window.getToOffset()>win.getWindowSpec().getToOffset())
					window.setToOffset(win.getWindowSpec().getToOffset());
				
			}
			else
			{
				window = new Window();
				window.setFromOffset(win.getWindowSpec().getFromOffset());
				window.setToOffset(win.getWindowSpec().getToOffset());
				window.setFromUnit(win.getWindowSpec().getFromUnit());
				window.setToUnit(win.getWindowSpec().getToUnit());
			}
		}
		else if (op instanceof OpUnary)
		{
			OpUnary unary = (OpUnary)op;
			getWindow(unary.getSubOp());
		}
		else if (op instanceof OpBinary)
		{
			OpBinary binary = (OpBinary)op;
			getWindow(binary.getLeft());
			getWindow(binary.getRight());
		}
		//else if (op instanceof OpMultiUnion)
		{
			
		}
	}
	
	public Window getWindow()
	{
		return window;
	}


	public String unAlias(String extent, String alias)
	{
		return projectionAlias.get(extent).get(alias);
	}

	public int getIndex(String alias)
	{
		String key = projectionAlias.keySet().iterator().next();
		int i =1;
		for (String columnalias:projectionAlias.get(key).keySet())
		{
			if (columnalias.equals(alias))
				return i;
			i++;
		}
		return 0;
	}

	public StandardCriterion[] getConditions()
	{
		return getConditions(operator).toArray(new StandardCriterion[0]);
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
