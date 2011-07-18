package es.upm.fi.dia.oeg.sparqlstream.syntax;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

public class ElementFunction extends Element
{

	Node var;
	Node parameterVar;
	
	public Node getParameterVar() {
		return parameterVar;
	}


	public void setParameterVar(Node parameterVar) {
		this.parameterVar = parameterVar;
	}

	public FunctionType type;
	
	public ElementFunction(Node var,Node parameterVar,FunctionType type)
	{
		setVar(var);
		setParameterVar(parameterVar);
		this.type = type;
	}
	
	
	@Override
	public boolean equalTo(Element el2, NodeIsomorphismMap isoMap) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void visit(ElementVisitor v) {
		// TODO Auto-generated method stub
		
	}

	public Node getVar() {
		return var;
	}

	public void setVar(Node var) {
		this.var = var;
	}
	
	

}
