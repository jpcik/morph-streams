package es.upm.fi.dia.oeg.sparqlstream.syntax;

import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;

import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

import es.upm.fi.dia.oeg.sparqlstream.WindowType;

public class ElementTimeWindow extends ElementWindow 
{
  
    private long range;
    private long delta;
    
    private WindowType type;
    
    private ExpressionTimeValue from;
    private ExpressionTimeValue to; 
    private TimeValue slide;
	
    
    
    public ExpressionTimeValue getFrom() {
		return from;
	}

	public void setFrom(ExpressionTimeValue from) {
		this.from = from;
	}

	public ExpressionTimeValue getTo() {
		return to;
	}

	public void setTo(ExpressionTimeValue to) {
		this.to = to;
	}

    public ElementTimeWindow(WindowType wt) {
        this.type = wt;
    }
    
    public ElementTimeWindow(long range, long delta, WindowType wt){
        this.range = range;
        this.delta = delta;
        this.type = wt;
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

    public long getRange() {
        return range;
    }

    public void setRange(long range, WindowUnit range_unit) {
        switch(range_unit){
	        case MILLISECOND:
	        case TRIPLE:
	            this.range = range;
	            break;
	            
	        case SECOND:
	            this.range = range * 1000;
	            break;
	        
	        case MINUTE:
	            this.range = range * 1000 * 60;
	            break;
	            
	        case HOUR:
	            this.range = range * 1000 * 60 * 60;
	            break;
	            
	        case DAY:
	            this.range = range * 1000 * 60 * 60 * 24;
	            break;
	            
	        case WEEK:
	            this.range = range * 1000 * 60 * 60 * 24 * 7;
	            break;
        }
    }

    public long getDelta() {
        return delta;
    }

    public void setDelta(long delta, WindowUnit delta_unit) {
    	if (delta_unit==null)
    		this.delta = delta;
    	else
    	{
    		switch(delta_unit){
    		case MILLISECOND:
    		case TRIPLE:
    			this.delta = delta;
    			break;

    		case SECOND:
    			this.delta = delta * 1000;
    			break;

    		case MINUTE:
    			this.delta = delta * 1000 * 60;
    			break;

    		case HOUR:
    			this.delta = delta * 1000 * 60 * 60;
    			break;

    		case DAY:
    			this.delta = delta * 1000 * 60 * 60 * 24;
    			break;

    		case WEEK:
    			this.delta = delta * 1000 * 60 * 60 * 24 * 7;
    			break;
    		}
    	}   }

	public WindowType getType() {
		return type;
	}

	public void setType(WindowType type) {
		this.type = type;
	}

	public void setSlide(TimeValue slide) {
		this.slide = slide;
	}

	public TimeValue getSlide() {
		return slide;
	}
    
}
