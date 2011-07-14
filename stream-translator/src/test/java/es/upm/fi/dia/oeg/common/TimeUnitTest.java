package es.upm.fi.dia.oeg.common;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

public class TimeUnitTest
{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}


	@Test
	public void testConvertToBase()
	{
		double d = TimeUnit.convertToBase(24, TimeUnit.HOUR);
		assertEquals(24*3600, d,0.0000001);
		d = TimeUnit.convertToBase(3000, TimeUnit.MILLISECOND);
		assertEquals(3, d,0.0000001);
	}

	@Test
	public void testConvertToUnit()
	{
		double d = TimeUnit.convertToUnit(24, TimeUnit.HOUR,TimeUnit.DAY);
		assertEquals(1, d,0.0000001);
		
	}

}
