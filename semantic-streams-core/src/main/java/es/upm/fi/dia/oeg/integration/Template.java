package es.upm.fi.dia.oeg.integration;

import java.util.Map;

import com.google.common.collect.Maps;


public class Template 
{
	private String modifier;
	private Map<String,String> modifiers;
	private String extent;
	//String modifier;
	
	public void addModifier(String varName,String modifier)
	{
		modifiers.put(varName, modifier);
	}
	
	public Template(String extent)
	{
		//this.setModifier(modifier);
		this.setExtent(extent);
		modifiers = Maps.newHashMap();
		//this.setVarName(varName);
	}
	
	public Map<String,String> getModifiers()
	{
		return modifiers;
	}

	public void setModifier(String modifier) {
		this.modifier = modifier;
	}

	public String getModifier() {
		return modifier;
	}

	public void setExtent(String extent) {
		this.extent = extent;
	}

	public String getExtent() {
		return extent;
	}
/*
	public void setVarName(String varName) {
		this.varName = varName;
	}

	public String getVarName() {
		return varName;
	}
	
*/
	@Override
	public String toString()
	{
		return extent+"-"+modifiers.toString();
	}
}
