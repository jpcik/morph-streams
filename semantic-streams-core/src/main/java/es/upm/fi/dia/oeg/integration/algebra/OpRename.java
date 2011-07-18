package es.upm.fi.dia.oeg.integration.algebra;

import java.util.Map;

import com.hp.hpl.jena.sparql.algebra.Op;

public class OpRename extends OpUnary
{

	private Map <String,String> substitutions;
	
	public OpRename(String id, OpInterface subOp)
	{
		super(id, "rename", subOp);
	}

	
}
