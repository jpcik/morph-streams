package es.upm.fi.dia.oeg.integration.adapter.snee;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.NullValue;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.sparql.algebra.Op;


import es.upm.fi.dia.oeg.common.TimeUnit;
import es.upm.fi.dia.oeg.integration.QueryBase;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.algebra.OpBinary;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpJoin;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpRoot;
import es.upm.fi.dia.oeg.integration.algebra.OpSelection;
import es.upm.fi.dia.oeg.integration.algebra.OpUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpWindow;
import es.upm.fi.dia.oeg.integration.algebra.Window;
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.dia.oeg.r2o.plan.Expression;
import es.upm.fi.dia.oeg.r2o.plan.ExpressionOperator;

public class SNEEqlQuery extends QueryBase implements SourceQuery
{
	public SNEEqlQuery()
	{
		streamList = new HashMap<String,String>();
		tableList = new HashMap<String,String>();
		projectList = new HashMap<String,Attribute>();
		conditionList = new ArrayList<String>();
		unionList = new HashMap<String,Set<String>>();
		inner = new HashMap<String,Map<String,SNEEqlQuery>>();
		condExpressions = new ArrayList<Expression>();
		
	}
	
	protected String innerQuery;
	
	public List<Expression> condExpressions;
	
	public Map<String,Map<String,SNEEqlQuery>> inner;
	
	private Map<String,Set<String>> unionList;
	//private Map<String,String> constants;
	//private Map<String,String> modifiers;
	private Map<String,String> streamList;
	public Map<String, Set<String>> getUnionList() {
		return unionList;
	}
	public void setUnionList(Map<String, Set<String>> unionExtents) {
		this.unionList = unionExtents;
	}

	public Map<String,String> getStreamList() {
		return streamList;
	}
	public void setStreamList(Map<String,String> streamList) {
		this.streamList = streamList;
	}
	public Map<String,String> getTableList() {
		return tableList;
	}
	public void setTableList(Map<String,String> tableList) {
		this.tableList = tableList;
	}
	public Map<String,Attribute> getProjectList() {
		return projectList;
	}
	/*
	public Map<String,String> getConstants()
	{
		return constants;
	}
	
	public Map<String, String> getModifiers()
	{
		return modifiers;
	}

	*/
	public Map<String,Attribute> getAugmentedProjectList()
	{
		
		for (Attribute att : projectList.values())
		{			
			Map<String,SNEEqlQuery> map = inner.get(extractExtent(att.getName()));
			for (SNEEqlQuery q:map.values())
			{
				Attribute at = q.projectList.get(att.getAlias());
				att.getInnerNames().add(at.getName().toLowerCase());
			}
		}
		return projectList;
		
	}
	
	
	public void setProjectList(Map<String,Attribute> projectList) {
		this.projectList = projectList;
	}
	public List<String> getConditionList() {
		return conditionList;
	}
	public void setConditionList(List<String> conditionList) {
		this.conditionList = conditionList;
	}
	
	public void addStreams(Map <String,String> streamMap)
	{
		this.streamList.putAll(streamMap);
	}
	
	public void addStream(String streamAlias, String streamName)
	{
		streamList.put(streamAlias, streamName);
	}
	
	public void addTable(String tableAlias, String tableName)
	{
		tableList.put(tableAlias, tableName);
	}
	private Map<String,String> tableList;
	private Map<String,Attribute> projectList;
	private List<String> conditionList;
	private int level;
	
	public void display()
	{
		display(0);
	}
	
	private String tab()
	{
		String res ="";
		for (int i=0;i<level;i++)
		{
			res += "\t";
		}
		return res;
	}
	
	public void display(int level)
	{
		this.level = level;

		//System.out.println(tab()+"UNION");
		//for (Map.Entry<String, Set<String>> entry : this.unionList.entrySet())
		//{
		//	System.out.print(entry.getKey()+",");
		//}
		System.out.print(tab()+"SELECT ");
		for (Attribute proj : projectList.values())
		{
			System.out.print(proj.getName()+",");
		}
		System.out.println();
		System.out.print(tab()+"FROM ");
		for (String proj : tableList.values())
		{
			System.out.print(proj+",");
		}
		System.out.println();
		System.out.print(tab()+"FROM STREAM ");
		for (String proj : streamList.values())
		{
			System.out.print(proj+",");
		}
		System.out.println();
		System.out.println(tab()+"WHERE");
		for (Expression proj : condExpressions)
		{
			System.out.print(proj.operator+",");
		}
		
		for (Map.Entry<String, Map<String,SNEEqlQuery>> list : inner.entrySet())
		{
			System.out.println(tab()+"at "+list.getKey());
			for (SNEEqlQuery sn:list.getValue().values())
			{
				sn.display(level+1);
			}
		}

	}
	
	private String trimExtent(String field)
	{
		int m = field.indexOf('.');
		
		String res = field.substring(m+1);
		return res;
	}
	
	private String extractExtent(String field)
	{
		int m = field.indexOf('.');
		
		String res = field.substring(0,m);
		return res;
	}
	
	private String replaceExtent(String field, String extent)
	{
		int m = field.indexOf('.');
		
		String res = extent+field.substring(m);
		return res;
	}
	
	private String serializeProjection()
	{
		String result = "SELECT "; 
			for (Map.Entry<String,Attribute> proj : projectList.entrySet())
			{
				result = result + trimExtent(proj.getValue().getName())+
				" AS "+proj.getKey()+
				",";
			}		
		//System.out.println("SELECT");
		return result;
	}

	private String buildCondition(Expression planExp,String oldExtent,String newExtent)
	{
		if (planExp.getValue() != null)
		{
			return planExp.getValue();
		}
		if (planExp.getVarName() != null)
		{
			int i = planExp.getAttributeURI().indexOf('.');
			if (oldExtent!=null && planExp.getAttributeURI().substring(0,i).equals(oldExtent))
				return newExtent + planExp.getAttributeURI().substring(i);
			else
				return planExp.getAttributeURI();
		}
		else if (planExp.getFirstMember()!=null)
		{
			String operator = " = ";
			if (planExp.operator==ExpressionOperator.GREATER) operator = " > ";
			else if (planExp.operator==ExpressionOperator.LESSER) operator = " < ";
			else if (planExp.operator==ExpressionOperator.AND) operator = " and ";
			else if (planExp.operator==ExpressionOperator.OR) operator = " or ";
			else if (planExp.operator==ExpressionOperator.ADD) operator = " + ";
			
			String condition1= buildCondition(planExp.getFirstMember(),oldExtent,newExtent);
			String condition2 =buildCondition(planExp.getSecondMember(),oldExtent,newExtent);
			return "("+condition1+") "+operator+ " ("+condition2+")";
		}
		return "";
	}
	@Override
	public String serializeQuery()
	{
		//return alternativeSerialization();
		return innerQuery;
	}
	@Override
	public Map<String, Attribute> getProjectionMap()
	{
		return getAugmentedProjectList();
	}
	
	@Override
	public void load(OpInterface op)
	{
		super.load(op);
		this.innerQuery = build(op);
	}
	
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
			String select = serializeSelect(proj);
			return "(SELECT "+ select+" FROM "+build(proj.getSubOp())+")";
		}
		else if (op instanceof OpWindow)
		{
			OpWindow win = (OpWindow)op;
			return win.getExtentName()+ serializeWindowSpec(win.getWindowSpec())+ " "+win.getExtentName();
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

	protected String serializeSelect(OpProjection proj)
	{
		return serializeSelect(proj, "");
	}
	
	protected String serializeExpressions(OpJoin join)
	{
		Map<String,String> varMappings = Maps.newHashMap();
		if (join.getLeft() instanceof OpProjection)
			 varMappings.putAll(((OpProjection)join.getLeft()).getVarMappings());
		if (join.getRight() instanceof OpProjection)
			 varMappings.putAll(((OpProjection)join.getRight()).getVarMappings());
		
		return serializeExpressions(join.getConditions(), varMappings);
	}

	private String serializeSelect(OpProjection proj,String index)	
	{
		return serializeSelect(proj, index, false);
	}
	protected String serializeSelect(OpProjection proj,String index, boolean fullExtent)
	{
		//OpProjection proj = (OpProjection)op;
		String select = "'"+proj.getRelation().getExtentName()+"' AS extentname"+index+", ";
		int pos =0;
		
		for (Map.Entry<String, Xpr> entry : proj.getExpressions().entrySet())
		{				
			if (entry.getValue()==ValueXpr.NullValueXpr)
			{
				pos++;
				continue;
			}
			String val = null;
			val = entry.getValue().toString();
			if (fullExtent)
				val = proj.getRelation().getExtentName()+"."+val;
			select += val+ " AS "+entry.getKey();

			if (pos < proj.getExpressions().size()-1) 
				select += ", ";
			pos++;
		}
		if (select.endsWith(", "))//TODO remove this ugly control
			select = select.substring(0,select.length()-2);
		
		return select;
	}
	
	private String unAlias(Xpr xpr,Map<String,String> varMappings)
	{
		if (varMappings == null)
			return xpr.toString();
		String unalias = "";
		if (xpr instanceof BinaryXpr)
		{
			BinaryXpr binary = (BinaryXpr)xpr;
			unalias = unAlias(binary.getLeft(),varMappings)+" "+binary.getOp()+" "+
					unAlias(binary.getRight(),varMappings);
		}
		else if (xpr instanceof VarXpr)
		{
			VarXpr var = (VarXpr)xpr;
			if (varMappings.containsKey(var.getVarName()))
				unalias =  varMappings.get(var.getVarName());
			else
				unalias = var.getVarName();
		}
		else if (xpr instanceof ValueXpr)
		{
			ValueXpr val = (ValueXpr)xpr;
			unalias = val.getValue();
		}
		return unalias;
	}
	
	protected String serializeExpressions(Collection<Xpr> xprs,Map<String,String> varMappings)
	{
		String exprs = "";
		int i=0;
		for (Xpr xpr:xprs)
		{						
			exprs+=unAlias(xpr,varMappings) 
				+(i+1<xprs.size()?" AND ":"");
			i++;
		}
		return exprs;
	}
	
	
	private String serializeWindowSpec(Window window)
	{
		if (window == null) return "";
		String ser = "[FROM NOW - "+window.getFromOffset()+" "+serializeTimeUnit(window.getFromUnit())+
					" TO NOW - "+window.getToOffset()+" "+serializeTimeUnit(window.getToUnit());
		if (window.getSlideUnit()!=null)
			ser +=	" SLIDE "+window.getSlide()+ " "+serializeTimeUnit(window.getSlideUnit());
		return ser+"]";
	}
	
	private String serializeTimeUnit(TimeUnit tu)
	{
		return tu.toString()+"S";
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
