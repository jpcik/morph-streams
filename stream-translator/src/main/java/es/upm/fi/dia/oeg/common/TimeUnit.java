package es.upm.fi.dia.oeg.common;

public enum TimeUnit 
{
	MILLISECOND(0.001),
	SECOND(1),
	MINUTE(60),
	HOUR(3600),
	DAY(3600*24),
	WEEK(3600*24*7),
	MONTH(3600*24*30),
	YEAR(3600*24*365);

	private double conversionFactor;
	
	TimeUnit(double factor)
	{
		conversionFactor = factor;
	}
	
	public static double convertToBase(double value, TimeUnit unit)
	{
		return value*unit.conversionFactor;
	}
	
	public static double convertToUnit(double value, TimeUnit unit, TimeUnit targetUnit)
	{
		double inBase = convertToBase(value, unit);
		return inBase/targetUnit.conversionFactor;
	}
}
