package es.upm.fi.dia.oeg.sparqlstream;

import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.*;
import com.hp.hpl.jena.sparql.function.FunctionEnv;
import com.hp.hpl.jena.sparql.graph.NodeTransform;

public class ExprTimestamp extends ExprFunction
{
	public static String TIMESTAMP_FUNCTION = "timestamp";
	
	private ExprVar paramVar;
	
	public ExprTimestamp(ExprVar paramVar)
	{
		super(TIMESTAMP_FUNCTION);
		this.paramVar = paramVar;
	}

	@Override
	public Expr getArg(int arg0) {
		// TODO Auto-generated method stub
		return paramVar;
	}

	@Override
	public int numArgs() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Expr copySubstitute(Binding arg0, boolean arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NodeValue eval(Binding arg0, FunctionEnv arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Expr applyNodeTransform(NodeTransform transform)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void visit(ExprVisitor visitor)
	{
		// TODO Auto-generated method stub
		
	}

}
