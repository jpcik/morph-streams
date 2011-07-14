package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Set;

import com.google.common.collect.Sets;


public class OpRelation extends OpUnary
{
	private static final String OP_RELATION = "relation";
	private String extentName;
	private Set<String> uniqueIndexes;

	public OpRelation(String id)
	{
		super(id, OP_RELATION, null);
		uniqueIndexes = Sets.newHashSet();		
	}

	
	public void setExtentName(String extentName)
	{
		this.extentName = extentName;
	}

	public String getExtentName()
	{
		return extentName;
	}

	@Override
	public String getString()
	{
		return getName()+" "+getExtentName();
	}
	
	@Override
	public OpRelation copyOp()
	{
		OpRelation copy = new OpRelation(getId());
		copy.setExtentName(getExtentName());
		copy.uniqueIndexes = Sets.newHashSet();
		copy.uniqueIndexes.addAll(uniqueIndexes);
		return copy;
	}
	

	@Override
	public String getName()
	{		
		return OP_RELATION;
	}


	public void setUniqueIndexes(Set<String> uniqueIndexes) {
		this.uniqueIndexes = uniqueIndexes;
	}


	public Set<String> getUniqueIndexes() {
		return uniqueIndexes;
	}
}
