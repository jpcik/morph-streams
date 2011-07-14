package es.upm.fi.dia.oeg.integration.translation;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.R2OConstants;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEqlQuery;
import es.upm.fi.dia.oeg.integration.algebra.Window;
import es.upm.fi.dia.oeg.r2o.ConceptMapDef;
import es.upm.fi.dia.oeg.r2o.OpNode;
import es.upm.fi.dia.oeg.r2o.R2OReader;
import es.upm.fi.dia.oeg.r2o.SlotMapDef;
import es.upm.fi.dia.oeg.r2o.plan.Acquire;
import es.upm.fi.dia.oeg.r2o.plan.Aggregation;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.dia.oeg.r2o.plan.BuiltFunction;
import es.upm.fi.dia.oeg.r2o.plan.Expression;
import es.upm.fi.dia.oeg.r2o.plan.ExpressionOperator;
import es.upm.fi.dia.oeg.r2o.plan.Join;
import es.upm.fi.dia.oeg.r2o.plan.Operation;
import es.upm.fi.dia.oeg.r2o.plan.OperationType;
import es.upm.fi.dia.oeg.r2o.plan.PlanNode;
import es.upm.fi.dia.oeg.r2o.plan.Projection;
import es.upm.fi.dia.oeg.r2o.plan.Selection;
import es.upm.fi.dia.oeg.r2o.plan.Union;
import es.upm.fi.dia.oeg.sparqlstream.StreamQuery;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementAggregate;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementStream;
import es.upm.fi.dia.oeg.sparqlstream.syntax.ElementTimeWindow;

public class R2OQueryTranslator extends QueryTranslator
{
	private R2OReader reader;

	private static Logger logger = Logger.getLogger(R2OQueryTranslator.class.getName());

	public R2OQueryTranslator(Properties props)
	{
		super(props);
	}

	public SNEEqlQuery translateAlter(String queryString, URI mappingUri) throws URISyntaxException, QueryException
	{
		System.out.println(queryString);
		StreamQuery query = null;
		try
		{
			query = (StreamQuery)StreamQueryFactory.create(queryString);
		}
		catch (Exception ex)
		{
			throw new QueryException("Error parsing query: "+queryString);
		}
		

		reader = R2OReader.getInstance(mappingUri);
		
		
		PlanNode n = new PlanNode( 
				"root",new Operation(OperationType.ROOT));
		Map<String,ConceptMapDef> con = visit(( ElementGroup)query.getQueryPattern());
		initTree(query, n, con);
		buildGraphTree(query,n,con);
		
		//displayTree(n);
		SNEEqlQuery sneeql = new SNEEqlQuery();
		
		translate2SNEEql(n,n, sneeql,mappingUri);
		sneeql.display();


		 
		 
		 logger.info("The translated query"+sneeql.serializeQuery());
		//demoLog.info(sneeql.serializeQuery());
		//sneeql.display();
		 reader.dispose();
		return sneeql;
	}

	
	private void translate2SNEEql(PlanNode root, PlanNode node,SNEEqlQuery query, URI mappingUri)
	{
		if (node==null) return;
		if (node.getOperation().isProjection())
		{
			Projection proj = (Projection)node.getOperation();
			for (Map.Entry<String, Attribute> attrEntry : proj.getAttributeUriMap().entrySet())
			{	
				query.getProjectList().put(attrEntry.getKey(), attrEntry.getValue());
			}
			for (Map.Entry<String,Expression> entry : proj.getExpressionMap().entrySet())
			{
				String coso = buildCondition(node,entry.getValue(),null,null);
				Attribute att = new Attribute();
				att.setAlias(entry.getKey());
				att.setName(coso);
				query.getProjectList().put(entry.getKey(), att);
			}
			
		}
		if (node.getOperation().getType() == OperationType.AGGREGATION)
		{
			Aggregation agg = (Aggregation)node.getOperation();
			for (Entry<String, BuiltFunction> aggEntry:agg.getFunctionAttributeMap().entrySet())
			{
				Attribute att = new Attribute();
				att.setAlias(aggEntry.getKey());
				att.setName(aggEntry.getValue().getFunctionName()+"("+ aggEntry.getValue().getParam()+ ")");
				query.getProjectList().put(aggEntry.getKey(), att);
			}
		}
		if (node.getOperation().isSelection())
		{
			Selection sel = (Selection)node.getOperation();
			for (Expression e : sel.expressions)
			{
				String condition = buildCondition(root, e, null,null);
				//query.getConditionList().add(condition);
				query.condExpressions.add(e);
/*
				String table = e.getExtent();
				Map<String,SNEEqlQuery> lista = query.inner.get(table);
				Set<String> extents = query.getUnionList().get(table);
				if (extents != null)
				for (String extent:extents)
				{
					SNEEqlQuery sneeql = lista.get(extent);
					String cond = buildCondition(root, e, table, extent);
					sneeql.getConditionList().add(cond);
					
				}
				*/
			}
			for (Map.Entry<String, Set<String>> entry:sel.unionConditions.entrySet())
			{
				query.getUnionList().put(entry.getKey(), entry.getValue());
			}

		
			Set<String> removeList = new HashSet<String>();
			for (Map.Entry<String,Set<String>> un : query.getUnionList().entrySet())
			{
			Map<String,SNEEqlQuery> listsn = new HashMap<String,SNEEqlQuery>();
			Set<String> set= un.getValue();
			for (String s:set)
			{
				SNEEqlQuery sneeql = new SNEEqlQuery();
				
				for (Map.Entry<String, Attribute> projItem:query.getProjectList().entrySet())
				{
					if (projItem.getValue().getName().startsWith(un.getKey()))
					{
						Attribute att = new Attribute();
						att.setAlias(projItem.getKey());
						att.setName(replaceExtent(projItem.getValue().getName(), s));
					sneeql.getProjectList().put(projItem.getKey(), att);
					}								
				}
				for (Map.Entry<String, String> streamItem:query.getStreamList().entrySet())
				{
					if (streamItem.getKey().equals(un.getKey()))
					sneeql.getStreamList().put(s, s);
				}
				List<Expression> expr = new ArrayList<Expression>();
				for (Expression e : query.condExpressions)
				{
					if (e.getExtent().equals(un.getKey()))
					//sneeql.getConditionList().add(buildCondition(root, e, un.getKey(), s));
					{
						Expression ex = replaceExtent(e, un.getKey(), s);
					sneeql.condExpressions.add(ex);
					removeList.add(un.getKey());
					}
					else
						expr.add(e);
				}
				
				//query.condExpressions = expr;
				if (sneeql.getProjectList().size()>0)
				  listsn.put(s,sneeql);
			}
			for (Map.Entry<String, Attribute> projItem:query.getProjectList().entrySet())
			{
				if (projItem.getValue().getName().startsWith(un.getKey()))
				{
					Attribute att = new Attribute(projItem.getKey(),un.getKey()+"."+projItem.getKey(),projItem.getValue().getDataType());
					query.getProjectList().put(projItem.getKey(),att);
				}
			}
			
            if (listsn.size()>0)
			query.inner.put(un.getKey(), listsn);
			}

			List<Expression> exps = new ArrayList<Expression>();
			for (Expression e:query.condExpressions)
			{
				if (!removeList.contains(e.getExtent()))
						exps.add(e);
			}
			query.condExpressions = exps;
		
		}
		if (node.getOperation().isJoin())
		{
			Join join = (Join)node.getOperation();			
			for (Expression e:join.expressions)
			{
			query.getConditionList().add(e.getFirstMember().getVarName() +"="+e.getSecondMember().getVarName());
			}
		}
		else if (node.getOperation().isAcquisition())
		{
			Acquire acq = (Acquire)node.getOperation();
			for (String s : acq.getExtents().values())
			{
					query.addTable(s,s);
			}			
			

		}
		else if (node.getOperation().getType() == OperationType.WINDOW)
		{
			Window win = (Window)node.getOperation();
				String stWind = "";
				for (String table: win.getExtents().values())
				{
						if (win.getFromUnit()!=null)
						{
						stWind = "[FROM NOW - "+win.getFromOffset()+" "+win.getFromUnit()+" TO NOW - "+win.getToOffset()+" "+win.getToUnit()+ " SLIDE "+win.getSlide()+" "+win.getSlideUnit()+" ]";
						}
						
						Map<String,SNEEqlQuery> lista = query.inner.get(table);
						if (lista!=null)
						{
						Set<String> extents = query.getUnionList().get(table);
						for (String extent:extents)
						{
							SNEEqlQuery sneeql = lista.get(extent);
							sneeql.addStream(extent, extent+stWind);
						}
						}
						else
							query.addStream(table,table+stWind);
							
						
				}
		}

		
		translate2SNEEql(root,node.getLeftNode(),query,mappingUri);
		translate2SNEEql(root,node.getRightNode(),query,mappingUri);
	}

	public Map<String,ConceptMapDef> visit( ElementGroup elementGroup)
	{
		
		Map<String,ConceptMapDef> conceptMap = new HashMap<String,ConceptMapDef>();
		//Map<String,Projection> attributeMap = new HashMap<String,Projection>();
		//Map<String,Join> relationMap = new HashMap<String,Join>(); 
		
		for (Element e: elementGroup.getElements())
		{
			if (e instanceof ElementTriplesBlock)
			{
				for (Triple t : ((ElementTriplesBlock)e).getPattern().getList())
				{
					if (t.getPredicate().getURI().equals(RDF.type.getURI()))
					{
						for (Object o:reader.getConceptMapDefs().values())
						{
							ConceptMapDef c = (ConceptMapDef)o;
							if (check(t.getSubject().getName(),elementGroup,c))
									{
									conceptMap.put(t.getSubject().getName(), c);
									}
							
						}
					}
				}
			}
			
		}
		logger.info(conceptMap.size()+"");
		return conceptMap;
	}
	
	private boolean check(String varname,ElementGroup g, ConceptMapDef con)
	{
		
		boolean varIsInMapping=true;
		
		//boolean matches = true;
		for (Element e: g.getElements())
		{
			if (e instanceof ElementTriplesBlock)
			{
				for (Triple t : ((ElementTriplesBlock)e).getPattern().getList())
				{					
					if (!t.getPredicate().getURI().equals(RDF.type.getURI()) &&	
							t.getSubject().getName().equals(varname))
						
					{
						boolean predicateIsInMapping = false;
						for (Object o:con.getSlotList().values())
							{
								SlotMapDef slot = (SlotMapDef)o;
								if (slot.getType()==R2OConstants.DBREL_TYPE && 
										slot.getMapName().equals(t.getPredicate().getURI()) &&
										!check(t.getObject().getName(),g,slot.getToConcept()))
									{predicateIsInMapping=false; break;}
								else if (slot.getMapName().equals(t.getPredicate().getURI()))
								{
									predicateIsInMapping=true; break;
								}
							}
						
						if (!predicateIsInMapping) 
							{varIsInMapping = false;break;} 
						
					}
					else if (t.getPredicate().getURI().equals(RDF.type.getURI()) && t.getSubject().getName().equals(varname))
					{
						if (!con.getMapName().equals(t.getObject().getURI()))
							return false;
					}
				}
				

			}
		}
		return varIsInMapping;
	}
	

	private boolean check(String varname,ElementGroup g, String conceptUri)
	{
		for (Element e: g.getElements())
		{
			if (e instanceof ElementTriplesBlock)
			{
				for (Triple t : ((ElementTriplesBlock)e).getPattern().getList())
				{
					
					if (t.getSubject().getName().equals(varname) && 
							t.getPredicate().getURI().equals(RDF.type.getURI()) && 
							t.getObject().getURI().equals(conceptUri))
						return true;
				}
			}
		}
		return false;
	}
	

	
	private void initTree(StreamQuery query, PlanNode root,Map<String,ConceptMapDef> conceptMappings)
	{
		 ElementGroup elementGroup = (ElementGroup)query.getQueryPattern();
		 
		 List<Var> projectVars = query.getProject().getVars();		 
		 Map<Var,Expr> projectExprs = query.getProject().getExprs();
     	 List<ElementAggregate> aggregates = query.getAggregates();
		
     	if (root.getNodeVarName().equals("root"))
     	{
     		Projection proj = null;
     		PlanNode projNode = null;
     		if (root.getChildNode()==null)
     		{
     			
     		
				proj = new Projection("projection");
				projNode = new PlanNode("projection",proj);
     			root.setChildNode(projNode);				
     		}     			
     		for (Var v : projectVars)
     		{
     			
     			proj.addAttributeUri(v.getVarName(), null,null);
     		}
     		if (!aggregates.isEmpty())
     		{
     			Aggregation agg = new Aggregation();
     			PlanNode aggNode = new PlanNode("aggregation",agg,projNode);
     			for (ElementAggregate aggElem : aggregates)
     			{
     				BuiltFunction func = new BuiltFunction(aggElem.getAggregateType().name(),aggElem.getVar().getName(),aggElem.getVarName().getName());
     				agg.addFunctionAttribute(aggElem.getVarName().getName(), func );
     			}
     			projNode.setChildNode(aggNode);
     		}
     		if (!projectExprs.isEmpty())
     		{
     			for (Map.Entry<Var,Expr> e : projectExprs.entrySet())
     			{
     				Expression planExp = new Expression();
     				buildExpression(e.getValue(), planExp);
     				logger.info(e.getKey().getVarName());
     				proj.getExpressionMap().put(e.getKey().getVarName(), planExp);
     			}
     		}
     		
			//prev = findNode(root, null,OperationType.PROJECTION);
			
			PlanNode inner = new PlanNode(projNode.getNodeVarName(),new Selection(((Projection)projNode.getOperation()).getConceptUri()),projNode);
			inner.setLeftNode(projNode.getLeftNode());
			if (projNode.getLeftNode()!=null)
				projNode.getLeftNode().setParent(inner);
			projNode.setLeftNode(inner);

			
     	}

		for (Element el: elementGroup.getElements())
		{
			if (el instanceof ElementGroup)
				buildGraphTree(query, root,conceptMappings);

			else if (el instanceof ElementFilter)
			{
				ElementFilter filter = (ElementFilter)el;
				Expr e =filter.getExpr();
				
				Expression planExp = new Expression();
				buildExpression(e, planExp);
				
				
					
					
					PlanNode prev = findNode(root, null,OperationType.SELECTION);
					if (prev == null)
					{
						prev = findNode(root, null,OperationType.PROJECTION);
												
						PlanNode inner = new PlanNode(prev.getNodeVarName(),new Selection(((Projection)prev.getOperation()).getConceptUri()),prev);
						inner.setLeftNode(prev.getLeftNode());
						if (prev.getLeftNode()!=null)
							prev.getLeftNode().setParent(inner);
						prev.setLeftNode(inner);
						prev = inner;
					}
					((Selection)prev.getOperation()).expressions.add(planExp);
		
	
			}
		}
	}
	
	private void buildGraphTree(StreamQuery query, PlanNode root,Map<String,ConceptMapDef> conceptMappings)
	{
		 ElementGroup elementGroup = (ElementGroup)query.getQueryPattern();
		 
		 List<ElementStream> streams = query.getStreams();
     	 List<ElementAggregate> aggregates = query.getAggregates();
		for (Element el: elementGroup.getElements())
		{
			if (el instanceof ElementGroup)
				buildGraphTree(query, root,conceptMappings);
			else if (el instanceof ElementTriplesBlock)
			{
				for (Triple t : ((ElementTriplesBlock)el).getPattern().getList())
				{
					SlotMapDef slotAttr = getMappedAttribute(t.getPredicate().getURI(),conceptMappings.get(t.getSubject().getName()) );
					SlotMapDef slotRel = getMappedRelation(t.getPredicate().getURI(),conceptMappings.get(t.getSubject().getName()));
					if ( slotAttr!=null && t.getObject().isVariable())
					{
						PlanNode proNode = findNode(root,null,OperationType.PROJECTION);
						Projection pro = (Projection)proNode.getOperation();
						if (pro.getAttributeUriMap().containsKey(t.getObject().getName()))
						{
							for (Object col : slotAttr.getUsedColumns())
							{
								pro.addAttributeUri(t.getObject().getName(), col.toString(),slotAttr.getDataType());break;//TODO add support for multi column
							}
						}
						for (Expression exp : pro.getExpressionMap().values())							
						{
							FillExpression(exp,t.getObject().getName(), slotAttr.getUsedColumns());							
						}

						PlanNode agNode = findNode(root,null,OperationType.AGGREGATION);
						if (agNode != null && !aggregates.isEmpty())
						{
						Aggregation ag = (Aggregation)agNode.getOperation();
						for (BuiltFunction fun : ag.getFunctionAttributeMap().values())
						{
							if (fun.getParamVarName().equals(t.getObject().getName()))
							{	
								for (Object col : slotAttr.getUsedColumns())
								{
									fun.setParam(col.toString());break;//TODO add support for multi-column;
								}
							}
						}
						}
						PlanNode selNode = findNode(root,null,OperationType.SELECTION);
						if (selNode!=null)
						{
							Selection sel = (Selection)selNode.getOperation();
							for (Expression exp :sel.expressions)
							{
								FillExpression(exp,t.getObject().getName(),slotAttr.getUsedColumns());
							}	 							
						}
						

					}
					else if (slotAttr!=null && t.getObject().isURI())
					{
						OpNode o = (OpNode)slotAttr.transfTree.getOpNodeList().get("0");
						OpNode o2 = (OpNode)o.getOpNodeList().get("then");
						if ( o2.getOp().getName().equals("extentname"))
						{
							OpNode oval = (OpNode)o2.getOpNodeList().get("value1");
							PlanNode selNode = findNode(root,null,OperationType.SELECTION);
							if (selNode!=null)
							{
								Selection sel = (Selection)selNode.getOperation();
								
								for (Map.Entry<String, Map<String,Set<String>>> ent : reader.getExtentMappings().entrySet())
								{
									Set<String> list = ent.getValue().get(t.getObject().getURI());
									if (list!=null)
										sel.unionConditions.put(ent.getKey(), list);
								}
								/*
								Map<String,Set<String>> extents = reader.getExtentMappings().get(oval.getContent());
								if (extents!=null)
								{
								Set<String> list = extents.get(t.getObject().getURI());
								sel.unionConditions.put(oval.getContent(), list);
								}
								*/
								
							}							
						}

					}
					else if (slotRel!=null)
					{
						
						PlanNode nod = findNode(root,t.getSubject().getName(),OperationType.ACQUISITION);
						
						
						Join join = new Join(t.getPredicate().toString(),"","");
						
						PlanNode n = new PlanNode(t.getSubject().toString(),join,nod.getParent());
						nod.getParent().setChildNode(n);
						n.setChildNode(nod);
						nod.setParent(n);
						
						if ( slotRel.condTree != null)
						{
						Expression e = new Expression();
						ExpressionOperator op = slotRel.condTree.getOp().getName().equals("equals")?ExpressionOperator.EQUALS:null;
						Expression v1 = new Expression();
						Expression v2 = new Expression();
						v1.setVarName(((OpNode)slotRel.condTree.getOpNodeList().get("value1")).getContent());
						v2.setVarName(((OpNode)slotRel.condTree.getOpNodeList().get("value2")).getContent());
	
						
						Object[] arr= slotRel.condTree.getTablesInvolved().keySet().toArray();
						// TODO other than binary join?			
						String tb1 = (String)arr[0];
						String tb2 = (String)arr[1];
						join.setSourceConceptUri(t.getSubject().getName());
						join.setTargetConceptUri(t.getObject().getName());
						
						e.setFirstMember(v1);
						e.setSecondMember(v2);
						e.operator = op;
						join.expressions.add(e); //TODO allow more expressions?
						}
					}
					else if (t.getPredicate().getURI().equals(RDF.type.getURI()))
					{						
						ConceptMapDef cDef = conceptMappings.get(t.getSubject().getName()); //getMappedConcept(t.getObject().getURI());
						if (cDef!=null)
						{
							
							PlanNode nod = findNode(root,t.getSubject().getName(),OperationType.JOIN);
							
							if (nod==null)
								nod = findLeafNode(root, OperationType.ANY);

				     		Union un = new Union();
				     		PlanNode unionNode = new PlanNode("union", un,nod);
				     		
				     		
							Acquire acq = new Acquire(t.getSubject().getName());

							if (cDef.getVirtualStream()==null)
								if (cDef.transfTree.getTablesInvolved()!=null)
								{
							for (Object tab : cDef.transfTree.getTablesInvolved().values())
							{
								acq.getExtents().put(tab.toString(), tab.toString());
								
								Map<String,Set<String>> l = reader.getExtentMappings().get(tab.toString());
								if (l!= null)
								{
									Set <String> exten = new HashSet<String>();
									for (Set<String> list : l.values())
									{
										for (String tableName : list)
										{
											exten.add(tableName);
										}
									}
									un.unionExtents.put(tab.toString(), exten);
								}
							}
								}

							
							PlanNode n = new PlanNode(t.getSubject().getName(), acq,unionNode);
							unionNode.setChildNode(n);
							if (nod.getOperation().isJoin())
								nod.setRightNode(unionNode);
							else
								nod.setChildNode(unionNode);
							for (ElementStream edst : streams)
							{
								if (edst.getUri().equals(cDef.getVirtualStream()))
								{
									List<String> tableList= reader.getTablesForVirtualStream(cDef.getVirtualStream());
									
									Window win = new Window(t.getObject().toString());
									for (Object tab : cDef.transfTree.getTablesInvolved().values())
									{
										
										for (String st:tableList)
										 	if (st.equals(tab.toString()))
											{
												win.getExtents().put(tab.toString(), tab.toString());
										
										Map<String,Set<String>> l = reader.getExtentMappings().get(tab.toString());
										if (l!= null)
										{
											Set<String> exten = new HashSet<String>();
											for (Set<String> list : l.values())
											{
												for (String tableName : list)
												{
													exten.add(tableName);
												}
											}
										 	un.unionExtents.put(tab.toString(), exten);
										}
											}
									}

									ElementTimeWindow sw = (ElementTimeWindow)edst.getWindow();
									if (sw !=null)
									{
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
									}
									PlanNode nw = new PlanNode(t.getSubject().toString(),win,n);
									n.setChildNode(nw);
								}
								
							}
							
							
			
						}
						
					}
				}
			}
		}
		
	}
	private String buildCondition(PlanNode root,Expression planExp,String oldExtent,String newExtent)
	{
		if (planExp.getValue() != null)
		{
			return planExp.getValue();
		}
		if (planExp.getVarName() != null)
		{
			if (planExp.getAttributeURI()==null)
				return planExp.getVarName();
			int i = planExp.getAttributeURI().indexOf('.');
			if (oldExtent!=null && planExp.getAttributeURI()!=null &&
					planExp.getAttributeURI().substring(0,i).equals(oldExtent))
			{
				return newExtent + planExp.getAttributeURI().substring(i);
			}
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
			
			String condition1= buildCondition(root,planExp.getFirstMember(),oldExtent,newExtent);
			String condition2 =buildCondition(root,planExp.getSecondMember(),oldExtent,newExtent);
			return "("+condition1+") "+operator+ " ("+condition2+")";
		}
		return "";
	}

	private String replaceExtent(String field, String extent)
	{
		int m = field.indexOf('.');
		
		String res = extent+field.substring(m);
		return res;
	}
	
	private Expression replaceExtent(Expression e, String oldExtent, String newExtent)
	{
		
		Expression ex = new Expression();
		ex.operator = e.operator;
		if (e.getVarName() != null)
		{
			ex.setVarName(e.getVarName());
			if (e.getAttributeURI()!=null)
			{
				int i = e.getAttributeURI().indexOf('.');
				if (oldExtent!=null &&
					e.getAttributeURI().substring(0,i).equals(oldExtent))
			{
				ex.setAttributeURI( newExtent + e.getAttributeURI().substring(i));
			}
			}
		}
		if (e.getValue()!= null)
		{
			
			ex.setValue(e.getValue());
		}
		if (e.getFirstMember()!=null)
		{
			ex.setFirstMember(replaceExtent(e.getFirstMember(), oldExtent, newExtent));
			ex.setSecondMember(replaceExtent(e.getSecondMember(), oldExtent, newExtent));
		}
		return ex;
	}

	private void buildExpression(Expr e, Expression planExp)
	{
		
		if (e instanceof ExprFunction2)
		{
			ExprFunction2 f2 = (ExprFunction2)e;
			ExpressionOperator op = null;
			
			if (f2.getOpName().equals("="))
				op = ExpressionOperator.EQUALS;
			else if (f2.getOpName().equals("<"))
				op = ExpressionOperator.LESSER;
			else if (f2.getOpName().equals(">"))
				op = ExpressionOperator.GREATER;
			else if (f2.getOpName().equals("&&"))
				op = ExpressionOperator.AND;
			else if (f2.getOpName().equals("||"))
				op = ExpressionOperator.OR;
			else if (f2.getOpName().equals("+"))
				op = ExpressionOperator.ADD;
			else if (f2.getOpName().equals("-"))
				op = ExpressionOperator.SUBSTRACT;
			else if (f2.getOpName().equals("*"))
				op = ExpressionOperator.MULTIPLY;
			else if (f2.getOpName().equals("/"))
				op = ExpressionOperator.DIVIDE;
			planExp.operator = op;
			
			Expression firstMember = new Expression();
			Expression secondMember = new Expression();
			
			buildExpression(f2.getArg1(), firstMember);
			buildExpression(f2.getArg2(), secondMember);
			planExp.setFirstMember(firstMember);
			planExp.setSecondMember(secondMember);
			
		}
		else if (e instanceof ExprVar)
		{
			planExp.setVarName(e.getVarName());
		}
		else if (e instanceof NodeValue)
		{
			planExp.setValue(((NodeValue) e).toString());
		}
	}

	
	private PlanNode findNode(PlanNode root, String varName, OperationType operationType)
	{
		PlanNode node = root;
		if (root == null) return null;
	
		
		
		if (varName == null && root.getOperation().getType()==operationType)
			return root;		
		else if (operationType==OperationType.ANY && root.getNodeVarName().equals(varName))
			return root;
		else if (operationType==OperationType.JOIN && root.getOperation().getType()==OperationType.JOIN)
		{
			Join j = (Join)root.getOperation();
			if (j.getTargetConceptUri().equals(varName))
				return root;
		}
		else if (root.getOperation().getType()==operationType &&
				root.getNodeVarName().equals(varName)) return root;
		{
			node = findNode(root.getLeftNode(),varName,operationType);
			if (node == null)
				node = findNode(root.getRightNode(), varName,operationType);
			return node;
		}
		//return node==null?root:node;
	}

	
	private PlanNode findLeafNode(PlanNode root, OperationType operationType)
	{
		PlanNode node = root;
		if (root == null) return null;
	
		if (root.getChildNode()==null && root.getOperation().getType()==operationType )
			return root;		
		else if (operationType==OperationType.ANY && root.getChildNode()==null)
			return root;
		node = findLeafNode(root.getLeftNode(),operationType);
		if (node == null)
			node = findLeafNode(root.getRightNode(),operationType);
		return node;
	}

	private SlotMapDef getMappedAttribute(String predicate, ConceptMapDef def)
	{
		return getMappedPredicate(predicate, R2OConstants.ATTMAP_TYPE, def);
	}

	private SlotMapDef getMappedRelation(String predicate, ConceptMapDef def)
	{
		return getMappedPredicate(predicate, R2OConstants.DBREL_TYPE,def);
	}
	
	private SlotMapDef getMappedPredicate(String predicate,int type, ConceptMapDef def)
	{
		if (def!=null)
		{
			SlotMapDef slot = def.getSlotByName(predicate);
			if (slot != null && slot.getType()==type)
			{
				return slot;
			}
		}
		return null;
	}

	private void FillExpression(Expression exp, String varName, Set usedColumns) 
	{
		
		if (exp.getVarName()!= null && exp.getVarName().equals(varName))
		{
			for (Object col : usedColumns)
			{
			exp.setAttributeURI(col.toString()); //TODO add multi column
			}
		}
		else if (exp.getFirstMember()!=null)
		{
			FillExpression(exp.getFirstMember(), varName, usedColumns);
			FillExpression(exp.getSecondMember(), varName, usedColumns);
		}
		
	}

}
