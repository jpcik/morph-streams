package es.upm.fi.dia.oeg.integration.algebra.xpr;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

public class ValueXprTest
{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}

	@Test
	public void testValueXpr()
	{
		ValueXpr val = new ValueXpr("val");
		assertEquals("val", val.getValue());
		val.setValue("vol");
		assertEquals("vol", val.getValue());
	}

	@Test
	public void testToString()
	{
		ValueXpr val = new ValueXpr("val");
		assertEquals("val", val.toString());
	}

	@Test
	public void testIsEqual()
	{
		ValueXpr val = new ValueXpr("aa");
		ValueXpr val1 = new ValueXpr("aa");
		ValueXpr val2 = new ValueXpr("bb");
		VarXpr var = new VarXpr("aa");
		
		assertTrue(val.isEqual(val1));
		assertFalse(val.isEqual(val2));
		assertFalse(val.isEqual(var));
		
	}

}
