package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import es.upm.fi.dia.oeg.integration.algebra.xpr.VarXpr;
import es.upm.fi.dia.oeg.integration.algebra.xpr.Xpr;

public class OpMultiUnion implements OpInterface
{
	private String id;
	private static final String OP_MULTIUNION = "multiUnion";
	private Map<String,OpInterface> children;
	//public Map<String,OpInterface> links;
	public Map<String,Multimap<String,String>> index;

	private static Logger logger = Logger.getLogger(OpMultiUnion.class.getName());

	
	public OpMultiUnion(String id)
	{
		setId(id);
		setChildren(new TreeMap<String,OpInterface>());
		//links = new TreeMap<String,OpInterface>();
		index = new TreeMap<String,Multimap<String,String>>();
	}
	
	
	
	@Override
	public OpInterface build(OpInterface op)
	{
		if (op instanceof OpMultiUnion)
		{
			OpMultiUnion multi = (OpMultiUnion)op;
			OpJoin join = new OpJoin(this, multi);
			return join;
		}
		else if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			OpJoin join = new OpJoin(this, proj);
			return join;
		}
		else	
			throw new NotImplementedException("we need to do this: "+op.toString());
	}

	@Override
	public OpInterface copyOp()
	{
		throw new NotImplementedException("we need to do this: ");
	}

	private String tab(int level)
	{
		String res ="";
		for (int i=0;i<level;i++)
		{
			res += "\t";
		}
		return res;
	}
	
	@Override
	public void display(int level)
	{
		logger.warn(tab(level)+getName()+" "+getId()+" "+index.keySet().toString());
		//if (children.size()<1000)
		//for (Map<String,OpInterface> mapp:index.values())
		int i=0;
		for (OpInterface op:children.values())
		{
			
			//for (OpInterface op:col)
			op.display(level+1);
			i++;
			if (i>20)
			break;
		}
	}

	@Override
	public void display()
	{
		
		display(0);
	}

	@Override
	public String getId()
	{
		return id;
	}

	@Override
	public String getName()
	{		
		return OP_MULTIUNION;
	}

	@Override
	public Map<String, Xpr> getVars()
	{
		Map<String,Xpr> vars = new HashMap<String,Xpr>();
		for (String k:index.keySet())
			vars.put(k, new VarXpr(k));
		/*
		for (OpInterface op:children.values())
		{	
			//for (OpInterface child:col)
			{
			OpInterface opi =(OpInterface) op;
			vars.putAll( opi.getVars());
			}
		}*/
		return vars;
	}

	@SuppressWarnings("unchecked")
	@Override
	public OpInterface merge(OpInterface op,Collection<Xpr> xprs)
	{
		if (op instanceof OpMultiUnion )
		{
			OpMultiUnion newUnion = new OpMultiUnion(id);
			OpMultiUnion union = (OpMultiUnion)op;
			Collection<String> seti = CollectionUtils.intersection(index.keySet(), union.index.keySet());
			logger.debug("merge unions: "+children.keySet()+"--"+union.children.keySet());
		
			Collection<String> set =  this.children.keySet(); //CollectionUtils.intersection(this.children.keySet(), union.children.keySet());
			for (String key:set)
			{
				OpInterface child =  children.get(key);
				for (String key2:union.children.keySet())
				{
					OpInterface child2 = union.children.get(key2);
					OpInterface opnew =child.copyOp().merge(child2.copyOp(),xprs);					
					if (opnew!=null)
						newUnion.children.put(key+key2, opnew);
				}
			}	
			if (true)
			return newUnion;

			
			//Collection<String> seti = CollectionUtils.intersection(index.keySet(), union.index.keySet());
			Collection<String> set1 = CollectionUtils.subtract(index.keySet(), union.index.keySet());

			for (String key:set1)
			{
				newUnion.index.put(key, index.get(key));
				logger.debug("key: "+index.get(key));
			}
			set1 = CollectionUtils.subtract(union.index.keySet(), index.keySet());
			for (String key:set1)
			{
				newUnion.index.put(key, union.index.get(key));
			}
			
			for (String key:seti)
			{							
				Multimap<String,String> mpp = index.get(key); 
				logger.debug("key: "+key+mpp+children.size());
				logger.debug("key2: "+union.index.get(key));
				Multimap<String,String> mpp2 = HashMultimap.create();
				Collection<String> setKey = mpp.keySet();
				for (String k:setKey)
				{
					Collection<String> ids = mpp.get(k);
					for (String id:ids)
					{
						OpInterface oi1 = children.get(id);
						if (oi1 == null)
							continue;
						if (union.index.get(key).get(k)==null)
							continue;
						Collection<String> ids2 = union.index.get(key).get(k);
						for (String id2:ids2)
						{
							OpInterface oi2 = union.children.get(id2);
							oi1.merge(oi2,xprs);
							mpp2.putAll(k, mpp.get(k));
							newUnion.children.put(id, oi1);
							for (String key2:set1)
							{
								for (Entry<String,String> e:newUnion.index.get(key2).entries())
								{
									if (e.getValue().equals(id2))
										newUnion.index.get(key2).put(e.getKey(), id);
								}
								//newUnion.index.get(key2).values().contains(o)
								//String tmapid = newUnion.index.get(key2).ge
								//newUnion.index.get(key2).put(tmapid,mpp.get(k) );
							}
						}
					}
					

				}
				
				newUnion.index.put(key,mpp2);
			}
			//if (true)
			return newUnion;

			/*
			Collection<String> set =  CollectionUtils.intersection(this.children.keySet(), union.children.keySet());
			for (String key:set)
			{
				OpInterface child =  children.get(key);
					OpInterface child2 = union.children.get(key);
						child.merge(child2,xprs);
					
				newUnion.children.put(key, child);							
			}	
			return newUnion;*/
		}
		else if (op instanceof OpProjection)
		{
			OpProjection proj = (OpProjection)op;
			logger.debug("extent "+proj.getRelation().getExtentName());
			Map<String,OpInterface> newChildren = new TreeMap<String,OpInterface>();
			for (Entry<String, OpInterface> child:children.entrySet())
			{
				OpInterface ch = child.getValue();
				OpInterface newchild = ch.merge(proj,xprs);
				if (newchild!=null)
					newChildren.put(child.getKey(), newchild);
			}
			this.setChildren(newChildren);
			return this;
		}
		else
			
			throw new NotImplementedException("Merge implementation missing: "+op.toString());
		
	}

	@Override
	public void setId(String id)
	{
		this.id = id;
		
	}

	@Override
	public void setName(String name)
	{	
		
	}



	public void setChildren(Map<String,OpInterface> children)
	{
		this.children = children;
	}



	public Map<String,OpInterface> getChildren()
	{
		return children;
	}
	

}
