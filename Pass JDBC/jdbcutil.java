/* jdbcutil.java
 *
 * Copyright (c) 1995-2011 Progress Software Corporation. All Rights Reserved.
 *
*/
package oajava.passjdbc;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import oajava.sql.*;

class jdbcutil {
    private long m_tmHandle = 0;
    private Connection con;
    private Statement stmt;
    private Exception lastException;

    final static int  IP_SUCCESS        = 0;    /* IP Routine was successful */
    final static int  IP_FAILURE        = 1;    /* IP Routine had an Error */

    public jdbcutil(long tmHandle) {
        m_tmHandle = tmHandle;
        con = null;
        stmt = null;
        lastException = null;
    }
    public int connect(String driver, String url, String uid, String password) {

        try {
	    clearException();
            Class.forName (driver);
            jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, "JDBC Driver:<" + driver + "> loaded successfully\n");

            try {
                    con = DriverManager.getConnection(url, uid, password);
            }
            catch(SQLException e) {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():SQLException in connecting to:<" + url + ">\n");
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in connecting(): "+ e + "\n");
                lastException = e;
                e.printStackTrace();
            }

            jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, "Successfully connected to:<" + url + ">\n");
            }
        catch(Exception e) {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in connecting to:<" + driver + ">\n");
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in connecting(): "+ e + "\n");
            lastException = e;
            e.printStackTrace();
            }
        finally {
                if(con == null) {
                    jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error in connecting to:<" + url + ">\n");
                    return IP_FAILURE;
                    }
            }

        if(con == null) return IP_FAILURE;

        return IP_SUCCESS;
    }

    public boolean isConnected() {
        return (con != null);
        }

    public boolean execute(String query) {
        try {
            clearException();
            stmt = con.createStatement();
            return (stmt.execute(query));
        } catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in execute(): "+ e + "\n");
            lastException = e;
        }
        return false;
    }

    public ResultSet getResultSet() {
        try {
	    clearException();
            return stmt.getResultSet();
        } catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getResultSet(): "+ e + "\n");
            lastException = e;
        }
        return null;
    }

    public int getUpdateCount() {
        try {
	    clearException();
            return stmt.getUpdateCount();
        } catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getUpdateCount(): "+ e + "\n");
            lastException = e;
        }
        return 0;
    }

    public int getColumnCount(ResultSet rs) {
        int numCols = 0;
        try {
	    clearException();
            ResultSetMetaData rsmd = rs.getMetaData ();
            numCols = rsmd.getColumnCount ();
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getColumnCount(): "+ e + "\n");
	    lastException = e;
        }
        return numCols;
    }

    public String getColumnValue(ResultSet rs,int i) {
        String str = null;
        try {
            str = rs.getString(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getColumnValue(" + i + ")"+ e + "\n");
        }
        return str;
    }

    public Object getColumnObject(ResultSet rs,int i) {
        Object obj = null;
        try {
            obj = rs.getObject(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getColumnObject(" + i + ")"+ e + "\n");
        }
        return obj;
    }

    public int getIntColumnValue(ResultSet rs,int i) {
        int val = -1;
        try {
        	val = rs.getInt(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getIntColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }
	
	public int getShortColumnValue(ResultSet rs,int i) {
        int val = -1;
        try {
        	val = rs.getShort(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getShortColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public BigDecimal getBigDecimalColumnValue(ResultSet rs,int i) {
    	BigDecimal val = null;
        try {
        	val = rs.getBigDecimal(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getBigDecimalColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public float getFloatColumnValue(ResultSet rs,int i) {
    	float val = -1;
        try {
        	val = rs.getFloat(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getFloatColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public double getDoubleColumnValue(ResultSet rs,int i) {
    	double val = -1;
        try {
        	val = rs.getDouble(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getDoubleColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public boolean getBooleanColumnValue(ResultSet rs,int i) {
    	boolean val = false;
        try {
        	val = rs.getBoolean(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getDoubleColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public Date getDateColumnValue(ResultSet rs,int i) {
    	Date val = null;
        try {
        	val = rs.getDate(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getDateColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public Time getTimeColumnValue(ResultSet rs,int i) {
    	Time val = null;
        try {
        	val = rs.getTime(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getTimeColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public Timestamp getTimeStampColumnValue(ResultSet rs,int i) {
    	Timestamp val = null;
        try {
        	val = rs.getTimestamp(i);
        } catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getTimeStampColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public Blob getBlobColumnValue(ResultSet rs,int i) 
    {
        Blob val = null;
        try 
        {
            val = rs.getBlob(i);
        } 
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getBlobColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public Clob getClobColumnValue(ResultSet rs,int i) 
    {
        Clob val = null;
        try 
        {
            val = rs.getClob(i);
        } 
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getClobColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public byte[] getBytesColumnValue(ResultSet rs,int i) 
    {
        byte[] val = null;
        try 
        {
            val = rs.getBytes(i);
        } 
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getBytesColumnValue(" + i + ")"+ e + "\n");
        }
        return val;
    }

    public void close()
    {
        if(con != null)
        {
            try
            {
                con.close();
                con = null;
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in close()"+ e + "\n");
            }
        }
    }

    public void commit()
    {
        try
        {
            con.commit();
        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in commit()"+ e + "\n");
        }
    }

    public void closeStmt()
    {
        if(stmt != null)
        {
            try
            {
                stmt.close();
                stmt = null;
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in closeStmt()"+ e + "\n");
            }
        }
    }

    public ResultSet getCatalogs()
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();
            return dbmd.getCatalogs();
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getCatalogs(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public ResultSet getSchemas()
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();
            return dbmd.getSchemas();
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getSchemas(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) 
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();
            return dbmd.getTables(catalog,schemaPattern,tableNamePattern,types);
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getTables(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public ResultSet getColumns(String catalog,String schemaPattern,String tableNamePattern,String columnNamePattern)
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();
            return dbmd.getColumns(catalog,schemaPattern,tableNamePattern,columnNamePattern);
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getColumns(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public ResultSet getPrimaryKeys(String catalog,String schema,String table)
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();
            return dbmd.getPrimaryKeys(catalog,schema,table);
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getPrimaryKeys(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public ResultSet getForeignKeys(String primaryCatalog,String primarySchema,String primaryTable,
                                    String foreignCatalog,String foreignSchema,String foreignTable)
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();

            if(primaryCatalog == null && primarySchema == null && primaryTable == null)
                return dbmd.getImportedKeys(foreignCatalog,foreignSchema,foreignTable);
            else
                if(foreignCatalog == null && foreignSchema == null && foreignTable == null)
                return dbmd.getExportedKeys(primaryCatalog,primarySchema,primaryTable);
            else
                return dbmd.getCrossReference(primaryCatalog,primarySchema,primaryTable,foreignCatalog,foreignSchema,foreignTable);            
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getForeignKeys(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public ResultSet getIndexInfo(String catalog,String schema,String table,boolean unique,boolean approximate)
    {
        try 
        {
            DatabaseMetaData dbmd = con.getMetaData();
	    clearException();
            return dbmd.getIndexInfo(catalog,schema,table,unique,approximate);
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getIndexInfo(): "+ e + "\n");
	    lastException = e;
        }
        return null;
    }

    public String jdbcGetInfo(int iInfoType)
    {
        String str = "";
        try 
        {
            switch (iInfoType)
            {
                case oajava.sql.ip.IP_INFO_QUALIFIER_NAMEW:
                    str = con.getCatalog();
                    break;
            }
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in jdbcGetInfo(): "+ e + "\n");
        }
        return str;
    }

    public void jdbcSetInfo(int iInfoType,String InfoVal)
    {
        try 
        {
            switch (iInfoType)
            {
                case oajava.sql.ip.IP_INFO_QUALIFIER_NAMEW:
                    con.setCatalog(InfoVal);
                    break;
            }
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in jdbcSetInfo(): "+ e + "\n");
        }
    }

    public String getColumnName(ResultSet rs,int i)
    {
        try 
        {
            ResultSetMetaData rsmd = rs.getMetaData ();
            return rsmd.getColumnName(i);
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getColumnName(): "+ e + "\n");
        }
        return "";
    }

    public int getColumnType(ResultSet rs,int i)
    {
        try 
        {
            ResultSetMetaData rsmd = rs.getMetaData ();
            return rsmd.getColumnType(i);
        } 
        catch(SQLException e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "jdbcutil():Exception in getColumnName(): "+ e + "\n");
        }
        return -999;
    }
    private void clearException()
    {
        lastException = null;
    }

    public Exception getLastException()
    {
        return lastException;
    }
}
