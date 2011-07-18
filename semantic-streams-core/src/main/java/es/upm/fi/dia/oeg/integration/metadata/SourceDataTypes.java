package es.upm.fi.dia.oeg.integration.metadata;
import java.sql.Types;

public enum SourceDataTypes {
	BOOLEAN (Types.BOOLEAN, "boolean"),
	DECIMAL (Types.DECIMAL, "decimal"),
	FLOAT (Types.FLOAT, "float"),
	INTEGER (Types.INTEGER, "integer"),
	TIMESTAMP (Types.TIMESTAMP, "timestamp"),
	VARCHAR (Types.VARCHAR, "string");
	
	private String display;
	private int type;

	SourceDataTypes(int type, String display) {
		this.type = type;
		this.display = display;
	}
	
	public int getSQLType() {
		return type;
	}
	
	public String toString() {
		return display;
	}
	
} 