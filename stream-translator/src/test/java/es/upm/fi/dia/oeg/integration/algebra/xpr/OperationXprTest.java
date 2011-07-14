package es.upm.fi.dia.oeg.integration.algebra.xpr;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

public class OperationXprTest
{

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}

	@Test
	public void testOperationXpr()
	{
		Xpr val = new ValueXpr("val");
		OperationXpr op = new OperationXpr("op", val);
		assertEquals("op", op.getOp());
		assertNotNull(op.getParam());
		assertSame(val, op.getParam());
	}

	@Test
	public void testSetParam()
	{
		OperationXpr op = new OperationXpr("op", null);
		op.setParam(new ValueXpr("val"));
		assertNotNull(op.getParam());
	}

	@Test
	public void testSetOp()
	{
		OperationXpr op = new OperationXpr("op", null);
		op.setOp("oper");
		assertEquals("oper", op.getOp());
	}

	@Test
	public void testToString()
	{
		OperationXpr op = new OperationXpr("constant",null);
		assertEquals("'null'", op.toString());
		op.setOp("Oper");
		assertEquals("Oper(null)", op.toString());
	}

	@Test
	public void testIsEqual()
	{
		OperationXpr o1 = new OperationXpr("constant", new ValueXpr("val"));
		OperationXpr o2 = new OperationXpr("constant", new ValueXpr("val"));
		OperationXpr o3 = new OperationXpr("boco", new ValueXpr("val"));
		OperationXpr o4 = new OperationXpr("constant", new ValueXpr("valo"));
		OperationXpr o5 = new OperationXpr("constant", new VarXpr("var"));
		VarXpr var = new VarXpr("vari");
		ValueSetXpr vs = new ValueSetXpr();
		vs.getValueSet().add("aa");
		vs.getValueSet().add("val");
		ValueSetXpr vs2 = new ValueSetXpr();
		vs2.getValueSet().add("aa");
		vs2.getValueSet().add("bb");
		
		assertTrue(o1.isEqual(o2));
		assertFalse(o1.isEqual(o3));
		assertFalse(o1.isEqual(o4));
		assertFalse(o1.isEqual(o5));
		assertFalse(o1.isEqual(vs2));
		assertTrue(o1.isEqual(vs));
		assertFalse(o1.isEqual(var));
		
		
		
		

		
	}

}
