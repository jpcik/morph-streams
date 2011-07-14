package es.upm.fi.dia.oeg.integration.algebra.xpr;

import java.util.Map;
import java.util.Set;

public interface Xpr
{

	Xpr copy();
	boolean isEqual(Xpr other);
	Set<String> getVars();
	void replaceVars(Map<String, String> varNames);
}
