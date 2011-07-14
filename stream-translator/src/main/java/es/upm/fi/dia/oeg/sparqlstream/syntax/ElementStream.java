package es.upm.fi.dia.oeg.sparqlstream.syntax;

import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;


public class ElementStream extends Element{
    

    private String uri;
    
    private ElementWindow window;
    
    public ElementStream() {
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public ElementWindow getWindow() {
        return window;
    }

    public void setWindow(ElementWindow window) {
        this.window = window;
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
    
}
