package es.upm.fi.dia.oeg.integration.algebra.xpr;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

public class VarXprTest
{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}


	@Test
	public void testVarXprString()
	{
		VarXpr var = new VarXpr("someVarName");
		assertEquals("someVarName", var.getVarName());
	}

	@Test
	public void testSetVarName()
	{
		VarXpr var = new VarXpr("aa");
		var.setVarName("bb");
		assertEquals("bb", var.getVarName());
	}

	@Test
	public void testGetVarName()
	{
		VarXpr var = new VarXpr("aa");
		String vn = var.getVarName();
		assertEquals("aa", vn);
		var.setVarName(null);
		assertNull(var.getVarName());
	}

	@Test
	public void testToString()
	{
		VarXpr var = new VarXpr("aa");
		String st = var.toString();
		assertEquals("aa", st);
	}

	@Test
	public void testIsEqual()
	{
		VarXpr var = new VarXpr("aa");
		VarXpr var2 = new VarXpr("bb");
		assertFalse(var.isEqual(var2));
		VarXpr var3 = new VarXpr("aa");
		assertTrue(var.isEqual(var3));
		ValueXpr val = new ValueXpr("aa");
		assertFalse(var.isEqual(val));
	}

	@Test
	public void testSetModifier()
	{
		VarXpr var = new VarXpr("aa");
		var.setModifier("ee");
		assertEquals("ee", var.getModifier());
	}

}
