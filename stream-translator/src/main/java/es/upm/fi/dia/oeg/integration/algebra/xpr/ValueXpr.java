package es.upm.fi.dia.oeg.integration.algebra.xpr;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public class ValueXpr implements Xpr
{
	private String value;

	public static ValueXpr NullValueXpr = new ValueXpr("NULL");
	
	public ValueXpr(String value)
	{
		setValue(value);
	}

	public void setValue(String value)
	{
		this.value = value;
	}

	public String getValue()
	{
		return value;
	}
	
	@Override
	public String toString()
	{
		return getValue();
	}

	@Override
	public boolean isEqual(Xpr other)
	{
		if (!(other instanceof ValueXpr))
			return false;
		ValueXpr valXpr = (ValueXpr)other; 
		return this.getValue().equals(valXpr.getValue());
	}

	@Override
	public Set<String> getVars()
	{
		return Sets.newHashSet();
	}

	@Override
	public void replaceVars(Map<String, String> varNames)
	{
		
	}

	@Override
	public Xpr copy()
	{
		ValueXpr copy = new ValueXpr(value);
		return copy;
	}
}

