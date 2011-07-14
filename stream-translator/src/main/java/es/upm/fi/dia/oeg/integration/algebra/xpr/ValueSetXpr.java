package es.upm.fi.dia.oeg.integration.algebra.xpr;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public class ValueSetXpr implements Xpr
{

	private Set<String> valueSet;
	
	public ValueSetXpr()
	{
		valueSet = new HashSet<String>();
	}
	
	public Set<String> getValueSet()
	{
		return valueSet;
	}
	
	@Override
	public boolean isEqual(Xpr other)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public String toString()
	{
		return valueSet.toString();
	}

	@Override
	public Set<String> getVars()
	{
		return Sets.newHashSet();
	}

	@Override
	public void replaceVars(Map<String, String> varNames)
	{
		//no vars
	}

	@Override
	public Xpr copy()
	{
		ValueSetXpr copy = new ValueSetXpr();
		copy.valueSet = Sets.newHashSet();
		copy.valueSet.addAll(valueSet);
		return copy;
	}
}
