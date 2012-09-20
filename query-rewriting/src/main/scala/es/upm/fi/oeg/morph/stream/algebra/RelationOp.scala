package es.upm.fi.oeg.morph.stream.algebra
import es.upm.fi.oeg.morph.common.TimeUnit


//import es.upm.fi.dia.oeg.integration.algebra.Window

class RelationOp(relationId:String,val extentName:String) 
  extends UnaryOp(relationId,"relation",null){
  
  override def toString=
	name+" "+extentName
	
  override def copyOp=
	new RelationOp(id,extentName)	
}

class WindowOp(windowId:String,extentName:String,val windowSpec:WindowSpec) 
  extends RelationOp(windowId,extentName){
  
  override def copyOp=
	new WindowOp(id, extentName,windowSpec)
	
  override def toString=
	"window"+" "+ extentName+ " "+windowSpec
	
}



	case class WindowSpec(iri:String,from:Long,fromUnit:TimeUnit,to:Long,toUnit:TimeUnit,slide:Long,slideUnit:TimeUnit) {
	/*public Window()
	{
		type = OperationType.WINDOW;
		extents = new HashMap<String, String>();
	}*/
	/*
	public Window(String iri)
	{
		this();
		setIri(iri);
	}*/
	/*
	private Map<String,String> extents;
	private String iri;
	private long range;
	private long slide;
	
	private long fromOffset;
	private long toOffset;
	
	private TimeUnit fromUnit;
	private TimeUnit toUnit;
	private TimeUnit slideUnit;*/
/*	
	public long getFromOffset() {
		return fromOffset;
	}
	public void setFromOffset(long fromOffset) {
		this.fromOffset = fromOffset;
	}
	public long getToOffset() {
		return toOffset;
	}
	public void setToOffset(long toOffset) {
		this.toOffset = toOffset;
	}
	public String getIri() {
		return iri;
	}
	public void setIri(String iri) {
		this.iri = iri;
	}
	public long getRange() {
		return range;
	}
	public void setRange(long range) {
		this.range = range;
	}
	public long getSlide() {
		return slide;
	}
	public void setSlide(long slide) {
		this.slide = slide;
	}
*/
	
	/*
	@Deprecated
	public long getRangeinUnit(TimeUnit unit)
	{
		switch (unit)
		{
		case SECOND:
			return range/1000;
		case MINUTE:
			return (range/1000)/60;
		case HOUR:
			return (range/1000)/3600;
		case DAY:
			return ((range/1000)/3600)/24;
		default:
			return range;
		}
		
			
	}*/
/*
	public TimeUnit getFromUnit() {
		return fromUnit;
	}

	public void setFromUnit(TimeUnit fromUnit) {
		this.fromUnit = fromUnit;
	}

	public static TimeUnit toTimeUnit(WindowUnit unit)
	{
		TimeUnit timeUnit = null;
		switch (unit)
		{
		case MILLISECOND:
			timeUnit = TimeUnit.MILLISECOND; break;
		case SECOND:
			timeUnit = TimeUnit.SECOND; break;
		case MINUTE:
			timeUnit = TimeUnit.MINUTE; break;
		case HOUR:
			timeUnit = TimeUnit.HOUR; break;
		case DAY:
			timeUnit = TimeUnit.DAY; break;
		case WEEK:
			timeUnit = TimeUnit.WEEK; break;
		case MONTH:
			timeUnit = TimeUnit.MONTH; break;
		case YEAR:
			timeUnit = TimeUnit.YEAR; break;

		}
		
		return timeUnit;
	}
	
	public void setFromUnit(WindowUnit fromUnit) 
	{
		setFromUnit(toTimeUnit(fromUnit));
	}

	public void setToUnit(WindowUnit toUnit) 
	{
		setToUnit(toTimeUnit(toUnit));
	}

	public TimeUnit getToUnit() {
		if (toUnit==null)
			return fromUnit;
		return toUnit;
	}

	public void setToUnit(TimeUnit toUnit) {
		this.toUnit = toUnit;
	}

	public void setExtents(Map<String,String> extents) {
		this.extents = extents;
	}

	public Map<String,String> getExtents() {
		return extents;
	}

	public void setSlideUnit(TimeUnit slideUnit) {
		this.slideUnit = slideUnit;
	}
	
	public void setSlideUnit(WindowUnit slideUnit) {
		setSlideUnit(toTimeUnit(slideUnit));
	}

	public TimeUnit getSlideUnit() {
		return slideUnit;
	}
	
	public String toString()
	{
		return "FROM "+getFromOffset()+" "+getFromUnit();
	}*/
}