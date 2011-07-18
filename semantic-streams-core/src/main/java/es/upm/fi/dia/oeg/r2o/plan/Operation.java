package es.upm.fi.dia.oeg.r2o.plan;

public class Operation 
{
	protected Operation()
	{
		
	}
	
	public Operation(OperationType type)
	{
		this.type = type;
	}
	
	protected OperationType type;

	public OperationType getType()
	{
		return type;
	}
	
	public boolean isJoin()
	{
		return type==OperationType.JOIN;
	}

	public boolean isAcquisition()
	{
		return type==OperationType.ACQUISITION;
	}

	public boolean isProjection()
	{
		return type==OperationType.PROJECTION;
	}

	public boolean isSelection()
	{
		return type==OperationType.SELECTION;
	}
	
	public boolean isRoot()
	{
		return type==OperationType.ROOT;
	}

	public boolean isAny()
	{
		return type==OperationType.ANY;
	}

	
}
