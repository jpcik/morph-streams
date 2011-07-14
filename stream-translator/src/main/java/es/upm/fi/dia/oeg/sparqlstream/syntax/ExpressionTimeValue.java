package es.upm.fi.dia.oeg.sparqlstream.syntax;


import es.upm.fi.dia.oeg.common.TimeUnit;

public class ExpressionTimeValue extends TimeValue 
{
	public ExpressionTimeValue(long offset,WindowUnit unit)
	{
		setOffset(offset);
		this.setUnit(unit);
	}
	
	private long offset;

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

}
