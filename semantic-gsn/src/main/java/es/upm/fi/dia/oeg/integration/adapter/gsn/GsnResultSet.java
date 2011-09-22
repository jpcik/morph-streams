package es.upm.fi.dia.oeg.integration.adapter.gsn;

import es.upm.fi.dia.oeg.integration.metadata.SourceDataTypes;
import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_DataField;
import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_QueryResult;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;

import org.apache.log4j.Logger;


import com.mysql.jdbc.ResultSetRow;

public class GsnResultSet implements ResultSet
{

	protected static Logger logger = Logger.getLogger(GsnResultSet.class.getName());
	private GSNWebService_QueryResult[] results;
	private Map<String,Object> row;
	private int current;
	private int currentStream;
	private GsnQuery query;
	
	public GsnResultSet(GSNWebService_QueryResult[] queryResults,GsnQuery gsnQuery)
	{
		row = new HashMap<String,Object>();
		query = gsnQuery;
		results = queryResults;
		currentStream = 0;
		current = -1;
	}
	

	@Override
	public boolean next() throws SQLException
	{
		while (results[currentStream].getStreamElements()==null)
		{		
			currentStream++;
			if (currentStream >=results.length)
				return false;
			
		}
		if (current+1>=results[currentStream].getStreamElements().length)			
		{
			if (currentStream+1 >= results.length)
				return false;
			else
			{
				if (results[currentStream+1].getStreamElements()==null)
					return false;
				currentStream++;
				current=0;
				return true;				
			}
		}		
		current++;
		
		return true;
		
	}
	
	@Override
	public boolean absolute(int row) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void afterLast() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeFirst() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void clearWarnings() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRow() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		return this.query.getIndex(columnLabel);


	}

	@Override
	public boolean first() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getConcurrency() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getCursorName() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHoldability() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		RowSetMetaData md = new RowSetMetaDataImpl();
	
		md.setColumnCount(query.getProjectionMap().size()+1);
		int i=1;
		for (String alias: query.getProjectionMap().keySet())
		{
			md.setColumnLabel(i, alias);
			md.setColumnType(i, SourceDataTypes.VARCHAR.getSQLType());
			i++;
		}
		md.setColumnLabel(i, "extentname");
		
		return md;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException
	{
		String label =this.getMetaData().getColumnLabel(columnIndex);
		return getObject(label);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException
	{
		String column = this.query.unAlias(results[currentStream].getVsname(), columnLabel);
		int i=0;int index =-1;
		logger.debug("Fetch from:" +results[currentStream].getVsname()+" columnLabel: "+columnLabel+ " column: "+column);
		if (columnLabel.equals("extentname"))
			return results[currentStream].getVsname();
		if (column.equals("timed"))
			return results[currentStream].getStreamElements()[current].getTimed();
		for (GSNWebService_DataField f:results[currentStream].getFormat().getField())
		{
			if (f.getName().toLowerCase().equals(column))
			{
				index =i;
				break;
			}
			i++;
		}
		if (index!=-1)
			return results[currentStream].getStreamElements()[current].getField()[index].getString();
		else
			throw new IllegalArgumentException("bad column "+columnLabel);
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRow() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(String columnLabel) throws SQLException
	{
		String column = this.query.unAlias(results[currentStream].getVsname(), columnLabel);
		int i=0;int index =-1;
		for (GSNWebService_DataField f:results[currentStream].getFormat().getField())
		{
			if (f.getName().equals(column))
			{
				index =i;
				break;
			}
			i++;
		}
		if (index!=-1)
		return results[currentStream].getStreamElements()[current].getField()[i].getString();
		else
			throw new RuntimeException("bad column");
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getType() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertRow() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLast() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean last() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean previous() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void refreshRow() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean relative(int rows) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setFetchSize(int rows) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNull(int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNull(String columnLabel) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRow() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public boolean wasNull() throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

}
