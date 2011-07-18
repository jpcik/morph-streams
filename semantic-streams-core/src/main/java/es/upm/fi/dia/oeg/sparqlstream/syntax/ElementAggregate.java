package es.upm.fi.dia.oeg.sparqlstream.syntax;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Variable;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

import es.upm.fi.dia.oeg.sparqlstream.AggregateType;

public class ElementAggregate extends Element
{

	AggregateType aggregateType;
	
	Node var;
	
	/**
	 * the name of the variable
	 */
	private Node varName;
	
	public ElementAggregate(Node var,Node varName, AggregateType type){
		this.var = var;
		this.aggregateType = type;
		setVarName(varName);
	}
	
	public Node getVar() {
		return var;
	}

	public void setVar(Node var) {
		this.var = var;
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

	public AggregateType getAggregateType() {
		return aggregateType;
	}

	public void setType(AggregateType type) {
		this.aggregateType = type;
	}

	public void setVarName(Node varName) {
		this.varName = varName;
	}

	public Node getVarName() {
		return varName;
	}
	
	//public String getVarName() {
	//	return Var.canonical(((Node_Variable)this.var).getName());
	//}

}
