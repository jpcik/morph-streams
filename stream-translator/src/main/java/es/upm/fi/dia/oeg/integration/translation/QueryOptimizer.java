package es.upm.fi.dia.oeg.integration.translation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.sparql.algebra.Op;

import es.upm.fi.dia.oeg.integration.algebra.OpBinary;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.algebra.OpJoin;
import es.upm.fi.dia.oeg.integration.algebra.OpMultiUnion;
import es.upm.fi.dia.oeg.integration.algebra.OpProjection;
import es.upm.fi.dia.oeg.integration.algebra.OpRelation;
import es.upm.fi.dia.oeg.integration.algebra.OpSelection;
import es.upm.fi.dia.oeg.integration.algebra.OpUnary;
import es.upm.fi.dia.oeg.integration.algebra.OpUnion;
import es.upm.fi.dia.oeg.integration.algebra.xpr.BinaryXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.OperationXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.ValueXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class QueryOptimizer
{
	private static Logger logger = Logger.getLogger(QueryOptimizer.class.getName());
	
	public OpInterface staticOptimize(OpBinary op)
	{
		OpInterface l = staticOptimize(op.getLeft());
		OpInterface r = staticOptimize(op.getRight());
		op.setLeft(l);
		op.setRight(r);
		
		if (op instanceof OpJoin)
		{
			OpJoin join = (OpJoin)op;
			if (l == null || r == null)
			{
				return null;  //join with empty relations is empty
			}
			else if (join.getLeft() instanceof OpProjection &&
				join.getRight() instanceof OpProjection)
			{
				/*
				OpProjection pl = (OpProjection)join.getLeft();
				OpProjection pr = (OpProjection)join.getRight();
				if (pl.getSubOp() instanceof OpRelation && pr.getSubOp() instanceof OpRelation)
				{
					OpRelation rl = (OpRelation)pl.getSubOp();
					OpRelation rr = (OpRelation)pr.getSubOp();
					if (join.hasEqualConditions())//(rl.getExtentName().equals(rr.getExtentName()))
					{
						logger.debug("Merge inner projections of join: "+pl+"-"+pr);
						for (String key : pr.getExpressions().keySet())//merge moth sides
						{
							if (pl.getExpressions().containsKey(key) && 
									pl.getExpressions().get(key) instanceof VarXpr)
							{
								VarXpr var = (VarXpr)pl.getExpressions().get(key);
								if (var.getModifier()!=null)
									continue;
							}
							pl.addExpression(key, pr.getExpressions().get(key));
						}
						return pl;
					}
					else if (rr.getExtentName().equals("constants")) //TODO test for left side
					{
						for (String key : pr.getExpressions().keySet())
						{
							pl.addExpression(key, pr.getExpressions().get(key));
						}
						return pl;
					}
					else if (!join.getConditions().isEmpty())
					{
						//throw new NotImplementedException("join with two dispaired projs");
					}
				}*/
			}
			else
			{
				logger.debug("join merging "+join.getConditions());
				join.getLeft().display();
				join.getRight().display();
				if (join.getConditions().isEmpty())
				{
					logger.debug("Cross product");
					return join;
				}

				if (join.getLeft() instanceof OpJoin)
				{
					OpJoin ljoin = (OpJoin)join.getLeft();
					if (ljoin.getConditions().isEmpty())// optimize empty cross prod
					{
						join.setLeft(ljoin.getRight());
						ljoin.setRight(join);
						return ljoin;
					}
				}
					
				if (join.hasEqualConditions())
				{
					OpInterface merged = join.getLeft().merge(join.getRight(),join.getConditions());
					//if (merged !=null)
					logger.debug("merged join");
						merged.display();
					return merged;
				}
				
				
				if (join.getLeft() instanceof OpMultiUnion)
					//join.getRight() instanceof OpMultiUnion) //push down join in unions
				{
					OpMultiUnion newunion = new OpMultiUnion(null);
					
					OpMultiUnion lunion = (OpMultiUnion)join.getLeft();
					
					
					Collection<OpInterface> rightOps = null;
					if (join.getRight() instanceof OpMultiUnion)
					{
						OpMultiUnion runion = (OpMultiUnion)join.getRight();
						rightOps = runion.getChildren().values();
					}
					else if (join.getRight() instanceof OpProjection)
					{
						rightOps = Lists.newArrayList();
						rightOps.add(join.getRight());
					}
					
					
					int i=0;
					for (OpInterface lo:lunion.getChildren().values())
					{
						for (OpInterface ro:rightOps)
						{
							OpJoin newjoin = new OpJoin(null,lo.copyOp(),ro.copyOp(), false );
							newjoin.getConditions().addAll(join.getConditions());
							newunion.getChildren().put("join"+i, newjoin);
							i++;
						}
					}
							
					return newunion;
				}
					
				/*
				else if (join.getRight() instanceof OpProjection)					
				{
					OpProjection proj = (OpProjection)join.getRight();
					if (proj.getRelation().getExtentName().equals("constants"))
					{
						OpInterface merged = join.getLeft().merge(join.getRight());				;
						return merged;
					}
				}*/
			}
		}
		else if (op.getName().equals("union"))
		{
			//if (r==null)
				//logger.info("fuckkkkkk");
		}
		/*
		if (l == null && r!= null)
			return r;
		else if (r==null && l != null)
			return l;
		else if (r == null & l == null) 
			return null;		
		else return op;*/
		return op;
	}
	public OpInterface staticOptimize(OpMultiUnion union)
	{
		logger.debug("optimize: "+union);
		//union.display();
		Set<String> toRemove = Sets.newHashSet();
		for (Entry<String,OpInterface> entry: union.getChildren().entrySet())
		{	
			OpInterface optimized = staticOptimize(entry.getValue());
			if (optimized!=null)
				union.getChildren().put(entry.getKey(),optimized );
			else
			{
				toRemove.add(entry.getKey());				
			}
		}
		for (String key:toRemove)
		{
			union.getChildren().remove(key);
			for (String k:union.index.keySet())
			{
				if (union.index.get(k).containsValue(key))
				{
					String rkey = null;
					for (String k2:union.index.get(k).keySet())
					{
						if (union.index.get(k).get(k2).contains(key))
							rkey = k2;
					}
					union.index.get(k).removeAll(rkey);
				}
				//throw new NotImplementedException("this key "+key);
				//union.index.get(k).inverse().remove(key);
			}
		}
		if (union.getChildren().size()==1)
		{
			logger.debug("simplified");
			//return union.getChildren().values().iterator().next();
		}
		//union.display();
		return union;
		
	}
	
	public OpInterface staticOptimize(OpUnary op)
	{
		OpInterface o =staticOptimize(op.getSubOp());
		op.setSubOp(o);
		
		if (op.getName().equals("projection"))
		{
			
			OpProjection proj = (OpProjection)op;
			if (proj.getSubOp()==null) //no relation to project
			{
				return null;
			}
			else if (proj.getSubOp().getName().equals("selection"))
			{
				OpSelection sel = (OpSelection)proj.getSubOp();
				logger.debug("remove constant selections");
				for (Xpr xpr:sel.getExpressions())
				{
				BinaryXpr cond = (BinaryXpr) xpr;
				if (cond.getOp().equals("=") && cond.getLeft() instanceof VarXpr)
				{
					VarXpr var = (VarXpr)cond.getLeft();  //so ugly code, we can refactor this mess
					if (proj.getExpressions().containsKey(var.getVarName()) && 
							proj.getExpressions().get(var.getVarName()) instanceof OperationXpr )
					{
						OperationXpr oper = (OperationXpr)proj.getExpressions().get(var.getVarName());
						if (oper.getOp().equals("constant"));
						{
							ValueXpr val = (ValueXpr)cond.getRight();
							if (val.getValue().equals(((ValueXpr) oper.getParam()).getValue()))
							{								
								proj.setSubOp(sel.getSubOp());
								return proj;
							}
							else
							{
								return null;
							}
						}
					}
				}
			
				}
				
			}
			else if (proj.getSubOp() instanceof OpBinary ) //push down projection in unions :)
			{
				OpBinary union = (OpBinary)proj.getSubOp();
				
				
				
				OpProjection leftProj = proj.copyOp();
				OpProjection rightProj = proj.copyOp();
				if (proj.getSubOp() instanceof OpJoin)
				{
					OpJoin join = (OpJoin)proj.getSubOp();
					for (Xpr xpr:join.getConditions())
					{
						for (String var:xpr.getVars())
						{
							if (!proj.getExpressions().containsKey(var))
							{
								leftProj.addExpression(var,new VarXpr(var));
								rightProj.addExpression(var,new VarXpr(var));
							}
						}
					}
				}
				leftProj.setSubOp(union.getLeft());
				rightProj.setSubOp(union.getRight());
				OpInterface l = staticOptimize(leftProj);
				OpInterface r = staticOptimize(rightProj);
				union.setLeft(l);
				union.setRight(r);
				//should simplify here!!!!
				return	union.simplify();
			
				//return union;
			}
			else if (o instanceof OpMultiUnion)
			{
				OpMultiUnion union = (OpMultiUnion)o;
				Map<String,OpInterface> newChildren = new TreeMap<String, OpInterface>();
				for (Map.Entry<String, OpInterface> col:union.getChildren().entrySet())
				{
					OpProjection copy= new OpProjection(proj.getId(),null);
					copy.getExpressions().putAll(proj.getExpressions());
					//Collection<OpInterface> newCol = new ArrayList<OpInterface>();
					//for (OpInterface opi:col.getValue())
					
						copy.setSubOp(col.getValue());
						OpInterface newOp = ( staticOptimize(copy));
					
					newChildren.put(col.getKey(),newOp);

				}
				union.setChildren(newChildren);
				return union;
			}
			else
			{
				if (o instanceof OpProjection ) //double projection, eat the inner!!!
				{
					logger.debug("merge double projections");
					OpProjection innerProj = (OpProjection)o;
					//boolean test = true;
					for (String key: proj.getExpressions().keySet())
					{
						if (innerProj.getExpressions().containsKey(key))
						{	
							proj.getExpressions().put(key, innerProj.getExpressions().get(key));
							//test=false;
							//break;
						}
						else //inner projection doesn't cover the attributes of the outer one
						{
							proj.getExpressions().put(key, ValueXpr.NullValueXpr);
						}
					}
					//if (test)
						proj.setSubOp(innerProj.getSubOp());
					//else
						//proj.setSubOp(o);
				}
				else if (o instanceof OpUnion)
				{
					System.out.println("cosos");
					/*
					OpUnion union = (OpUnion)o;
					OpInterface l = staticOptimize(union.getLeft());
					OpInterface r = staticOptimize(union.getRight());
					union.setRight(r);
					union.setLeft(l);
					return union;*/
				}
				else if (o!=null)  
					proj.setSubOp(o);
				else   // if projection has no relation to project!!!
					return null;
				
				return proj;
			}
		}
		else if (op instanceof OpSelection)
		{
			OpSelection selection = (OpSelection)op;
			//BinaryXpr bin = (BinaryXpr) xpr;
			//VarXpr var = (VarXpr) bin.getLeft();
			if (op.getSubOp() instanceof OpProjection) //apply the selection to the projection
			{
				OpProjection proj = (OpProjection)op.getSubOp();
				for (String varName:selection.getSelectionVars())
				{
					if (proj.getExpressions().containsKey(varName))
					{
						OpSelection sel = new OpSelection(selection.getId(),null,selection.getExpressions());
						proj.build(sel);
						return proj;
					}
					else
						return null;
				}
			}
			else if (op.getSubOp() instanceof OpUnion)
			{
				// push down selection to union children
				OpUnion union = (OpUnion)op.getSubOp();
				OpSelection selLeft = new OpSelection(selection.getId(),union.getLeft(),selection.getExpressions());
				//selLeft.getExpressions().addAll(selection.getExpressions());
				OpSelection selRight = new OpSelection(selection.getId(),union.getRight(),selection.getExpressions());
				//selRight.getExpressions().addAll(selection.getExpressions());
				OpInterface left = staticOptimize(selLeft);
				OpInterface right = staticOptimize(selRight);
				return new OpUnion(left, right).simplify();
				
			}
			else if (op.getSubOp() instanceof OpMultiUnion)
			{
				OpMultiUnion union = (OpMultiUnion)op.getSubOp();
				TreeMap<String,OpInterface> newChildren = new TreeMap<String, OpInterface>();
				for (Map.Entry<String, OpInterface> col:union.getChildren().entrySet())
				{
					//Collection<OpInterface> col2 = new ArrayList<OpInterface>();
					//for (OpInterface opi:col.getValue())
					
					OpSelection sel = new OpSelection(selection.getId(),col.getValue(),selection.getExpressions());
					//sel.getExpressions().addAll(selection.getExpressions());
					OpInterface newChild = staticOptimize(sel);
					//col2.add(newChild);
					
					newChildren.put(col.getKey(), newChild);
				}
				union.setChildren(newChildren);
				return union;
			}
			else if (op.getSubOp() instanceof OpJoin)
			{
				OpJoin join = (OpJoin)op.getSubOp();
				join.getConditions().addAll(selection.getExpressions());
				return join;
				//throw new NotImplementedException("Not done for joins");
			}
			
		}
		else
		{
			OpInterface oi =staticOptimize(op.getSubOp());
			op.setSubOp(oi);
			return op;
		}
		return op;
	}
	
	public OpInterface staticOptimize(OpInterface op)
	{
		if (op instanceof OpUnary)
			return staticOptimize((OpUnary)op);
		else if (op instanceof OpBinary)
			return staticOptimize((OpBinary)op);
		else if (op instanceof OpMultiUnion)
			return staticOptimize((OpMultiUnion)op);
		
		
		
		return op;
	}
}
