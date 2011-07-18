package es.upm.fi.dia.oeg.integration.algebra.xpr;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import es.upm.fi.dia.oeg.integration.algebra.OpProjection;

public class VarXpr implements Xpr
{
	private String varName;
	private String modifier;
	//private Set<String> addVars; 
	
	private static Logger logger = Logger.getLogger(VarXpr.class.getName());

	
	public VarXpr(String varName)
	{
		setVarName(varName);
		//addVars = new HashSet<String>();
	}

	public void setVarName(String varName)
	{
		this.varName = varName;
	}

	public String getVarName()
	{
		return varName;
	}
	
	@Override
	public String toString()
	{
		//logger.debug("modifiers "+getModifier());
		return getVarName();
	}

	@Override
	public boolean isEqual(Xpr other)
	{
		if (!(other instanceof VarXpr))
			return false;
		VarXpr othervar = (VarXpr)other;
		return this.getVarName().equals(othervar.getVarName());
	}

	public void setModifier(String modifier)
	{
		this.modifier = modifier;
		
	}

	public String getModifier()
	{
		return this.modifier;
	}
/*
	public void setAddVars(Set<String> addVars)
	{
		this.addVars = addVars;
	}

	public Set<String> getAddVars()
	{
		return addVars;
	}

*/

	@Override
	public Set<String> getVars()
	{
		return Sets.newHashSet(varName);
	}

	@Override
	public void replaceVars(Map<String, String> varNames)
	{
		if (varNames.containsKey(varName))
			varName = varNames.get(varName);
		
	}

	@Override
	public Xpr copy()
	{
		VarXpr copy = new VarXpr(varName);
		copy.modifier = modifier;
		return copy;
		
	}


}
