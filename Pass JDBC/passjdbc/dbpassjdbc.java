/* dbpassjdbc.java
 *
 * Copyright (c) 1995-2011 Progress Software Corporation. All Rights Reserved.
 *
 *
 * Description:     This Java example IP uses PassThrough Mode to build and send the entire
 *                  query to a backend JDBC datasource
 *                  - uses dynamic schema
 *
 *                  The example can be run in two modes (Default mode is PushPostProcessing)
 *                  PushPostProcessing - handle post processing by backend (GROUP BY, ORDER BY, SET functions, DISTINCT)
 *                  No PushPostProcessing - Allow DAM to handle it
 *                  This is controlled by openrda.ini setting
 *                  [PASSIP]
 *                  PushPostProcessing=1
 */

package oajava.passjdbc;

import java.lang.reflect.Array;
import java.math.*;
import java.sql.*;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Calendar;
import oajava.sql.*;

import java.util.StringTokenizer;
import java.io.*;
import java.nio.*;

/* define the class dbpassjdbc to implement the sample IP */
public class dbpassjdbc implements oajava.sql.ip
{
    final static int MAX_QUERY_LEN = 1024;

    private long m_tmHandle = 0;
    private int  m_iNumResRows;

    private StringBuffer    m_sWorkingDir; /* [DAM_MAXPATH]; */
    private boolean         m_bPushPostProcessing; /* Indicates if Post-processing (GROUP BY, ORDER BY etc)
                                                    should be sent to back-end or allow DAM to process */
    private boolean         m_bUseOriginalSelectList; /* Indicates if query sent to backend should use original select list
                                                        expressions or just columns in use. When original query
                                                        has GROUP BY, SET functions and  gbPushPostProcessing is FALSE,
                                                        IP should return values for base columns so that DAM can do the post-processing
                                                      */
    private boolean         m_bIgnoreDateTimeParseError; /* Indicates to Ignore Date/Time/TimeStamp lilteral error validation (PSC00055439) */
	private boolean         m_bValidateNullConstraint; /* Integrity constraint violation. NULL value specified for NON-NULLABLE column (40016) */
    private jdbcutil        m_JdbcUtil;
    private XXX_STMT_DA[]   m_stmtDA;




    /* connection information */
    private StringBuffer    m_sIniFile;
    private StringBuffer    m_sQualifier;         /* char [DAM_MAX_ID_LEN+1] */
    private StringBuffer    m_sUserName;          /* char [DAM_MAX_ID_LEN+1] */


    final static String OA_CATALOG_NAME   = "SCHEMA";        /* SCHEMA */
    final static String OA_USER_NAME      = "OAUSER";        /* OAUSER */

    PASSJDBC_PROC_DA[]           procDA;

    final static int FILEIO_MAX_BUFFER_SIZE = 10240; /* 65534*/        /* bytes */
    private boolean         m_bUseBulkFetch; /* true means use bulk result Fetch for build rows */
    /* Support array */
    private final int[]   ip_support_array =
                    {
                        0,
                        1, /* IP_SUPPORT_SELECT */
                        1, /* IP_SUPPORT_INSERT */
                        1, /* IP_SUPPORT_UPDATE */
                        1, /* IP_SUPPORT_DELETE */
                        1, /* IP_SUPPORT_SCHEMA - IP supports Schema Functions */
                        0, /* IP_SUPPORT_PRIVILEGES  */
                        1, /* IP_SUPPORT_OP_EQUAL */
                        0, /* IP_SUPPORT_OP_NOT   */
                        0, /* IP_SUPPORT_OP_GREATER */
                        0, /* IP_SUPPORT_OP_SMALLER */
                        0, /* IP_SUPPORT_OP_BETWEEN */
                        0, /* IP_SUPPORT_OP_LIKE    */
                        0, /* IP_SUPPORT_OP_NULL    */
                        0, /* IP_SUPPORT_SELECT_FOR_UPDATE */
                        0, /* IP_SUPPORT_START_QUERY */
                        0, /* IP_SUPPORT_END_QUERY */
                        0, /* IP_SUPPORT_UNION_CONDLIST */
                        1, /* IP_SUPPORT_CREATE_TABLE */
                        1, /* IP_SUPPORT_DROP_TABLE */
                        0, /* IP_SUPPORT_CREATE_INDEX */
                        0, /* IP_SUPPORT_DROP_INDEX */
                        1, /* IP_SUPPORT_PROCEDURE */
                        1, /* IP_SUPPORT_CREATE_VIEW */
                        1, /* IP_SUPPORT_DROP_VIEW */
                        1, /* IP_SUPPORT_QUERY_VIEW */
                        0, /* IP_SUPPORT_CREATE_USER */
                        0, /* IP_SUPPORT_DROP_USER */
                        0, /* IP_SUPPORT_CREATE_ROLE */
                        0, /* IP_SUPPORT_DROP_ROLE */
                        0, /* IP_SUPPORT_GRANT */
                        0, /* IP_SUPPORT_REVOKE */
                        1,  /* IP_SUPPORT_PASSTHROUGH_QUERY */
                        1,  /* IP_SUPPORT_NATIVE_COMMAND */
                        0,  /* IP_SUPPORT_ALTER_TABLE */
                        0,  /* IP_SUPPORT_BLOCK_JOIN */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0,  /* Reserved for future use */
                        0   /* Reserved for future use */
                    };


    public dbpassjdbc()
    {
            m_tmHandle = 0;
            m_iNumResRows = 0;

            m_sQualifier = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
            m_sUserName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
            m_sWorkingDir = new StringBuffer(ip.DAM_MAXPATH + 1);

            m_sIniFile = new StringBuffer(256 + 1);

            m_bPushPostProcessing    = false;
            m_bUseOriginalSelectList = true;
            m_bIgnoreDateTimeParseError = false;
			m_bValidateNullConstraint = false;

            m_JdbcUtil = null;

            m_stmtDA = new XXX_STMT_DA[50];   /* Array elements will be created when required */

	    procDA = new PASSJDBC_PROC_DA[10];   /* Array elements will be created when required */

	    m_bUseBulkFetch = false;
    }

    public String ipGetInfo(int iInfoType)
    {
        String str = null;
        switch (iInfoType)
                {
		   case IP_INFO_QUALIFIER_NAMEW:
	              if(m_JdbcUtil == null)
			  str = m_sQualifier.toString();
		      else
                          str = m_JdbcUtil.jdbcGetInfo(iInfoType);

		      break;

		    case IP_INFO_SUPPORT_SCHEMA_SEARCH_PATTERN:
			 str = "1"; /* true */
			 break;

		   case IP_INFO_VALIDATE_NULL_CONSTRAINT:
					if ( m_bValidateNullConstraint )
                            str = "1"; /* true */
                    else
                            str = "0"; /* false */
                    break;
			 

                    case IP_INFO_SUPPORT_VALUE_FOR_RESULT_ALIAS:
                    case IP_INFO_VALIDATE_TABLE_WITH_OWNER:
                    case IP_INFO_FILTER_VIEWS_WITH_QUALIFIER_NAME:
                    case IP_INFO_CONVERT_NUMERIC_VAL:
                    case IP_INFO_TABLE_ROWSET_REPORT_MEMSIZE_LIMIT:
                        str = "0"; /* false */
                        break;
                    case IP_INFO_IGNORE_DATETIME_PARSE_ERROR:
                        if ( m_bIgnoreDateTimeParseError )
                            str = "1"; /* true */
                        else
                            str = "0"; /* false */
                        break;

                    case IP_INFO_OWNER_NAMEW:
                        /* we need to return information that matches schema */
                        str = m_sUserName.toString();
						break;
                }
                return str;
        }

    public int ipSetInfo(int iInfoType,String InfoVal)
    {
        switch(iInfoType)
        {
            case IP_INFO_QUALIFIER_NAMEW:
                m_sQualifier.delete(0, m_sQualifier.length());
                m_sQualifier.append(InfoVal);
		if(m_JdbcUtil != null)
		    m_JdbcUtil.jdbcSetInfo(iInfoType,InfoVal);
                break;
        }

        return IP_SUCCESS;
    }

    public int ipGetSupport(int iSupportType)
    {
        return(ip_support_array[iSupportType]);
    }

        /*ipConnect is called immediately after an instance of this object is created. You should
         *perform any tasks related to connecting to your data source */
    public int        ipConnect(long tmHandle, long dam_hdbc, 
                                String sDataSourceName, String sUserName, String sPassword,
    							String sCurrentCatalog, String sIPProperties, String sIPCustomProperties)
        {
            int             iRetCode = IP_SUCCESS;
            StringBuffer    strInfo =  new StringBuffer(ip.DAM_MAX_ID_LEN+1);
            xo_int          piValue = null;
            String          driver = null;
            String          url = null;
            StringTokenizer st;

            /* Save the trace handle */
            m_tmHandle = tmHandle;
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipConnect called\n");

            m_sQualifier.delete(0, m_sQualifier.length());
            m_sQualifier.append(OA_CATALOG_NAME);
            m_sUserName.delete(0, m_sUserName.length());
            m_sUserName.append(OA_USER_NAME);
            /* m_sUserName.append(sUserName); */

            /* get openrda.ini and read configuration information */
            iRetCode = jdam.dam_getInfo(dam_hdbc, 0, DAM_INFO_OPENRDA_INI,
                                            m_sIniFile, null);
        jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "[PushJDBC]OASQL.INI=<" + m_sIniFile.toString() + ">\n");
            ip_read_configuration();


            /* Code to connect to your data source source. */

            /* Push post processing to the back-end */
            if(m_bPushPostProcessing)
               jdam.dam_setOption(DAM_CONN_OPTION, dam_hdbc, DAM_CONN_OPTION_POST_PROCESSING, DAM_PROCESSING_OFF);
               
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"PushPostProcessing = "+  m_bPushPostProcessing + "\n");

		    jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, "psDataSourceIPProperties= " + sIPProperties + "\n");
		    jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, "psDataSourceIPCustomProperties=" + sIPCustomProperties + "\n");


        /* Read JDBC driver connect string  */
	    try
	    {
            if(driver == null && url == null && sIPCustomProperties.length() != 0)
            {
                StringTokenizer stCust = new StringTokenizer(sIPCustomProperties,"=;");
                while(stCust.hasMoreTokens()) 
                {
                    String Key = stCust.nextToken();
                    if(Key.equalsIgnoreCase("JDBC_DRIVER"))
                        driver = stCust.nextToken().trim();
                    if(Key.equalsIgnoreCase("JDBC_URL"))
                    {
                        url = stCust.nextToken().trim();
                        break;
                    }
                }
            /* Format URL to have the entire string after JDBC_URL. This allows URL to have key=value settings */
		    String tempUrl = url;
		    url = tempUrl + sIPCustomProperties.substring((sIPCustomProperties.indexOf(tempUrl) + tempUrl.length()));
		}
	    }
	    catch (Exception e)
            {
                iRetCode = IP_FAILURE;
                return iRetCode;
            }

	    jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, "dburl= " + url + "\n");
            
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"Connecting to JDBCDriver:<" + driver + ">. URL:<" + url + ">\n");
    
        if (driver != null && url == null) 
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Invalid connect string:<" + strInfo + ">\n");
            jdam.dam_addError(dam_hdbc, 0, 1, 0, "Invalid connect string:<" + strInfo + ">");
            return IP_FAILURE;
        }

        if (driver != null) 
        {
            jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, " *** Connection will be setup for full Execution Mode *** \n");
            m_JdbcUtil = new oajava.passjdbc.jdbcutil(tmHandle);
        }
        else 
        {
            jdam.trace(m_tmHandle, ip.UL_TM_MAJOR_EV, " *** Driver Keyword missing. Operating in Format-Only Mode *** \n");
            m_JdbcUtil = null;
        }

        if(m_JdbcUtil != null)
            iRetCode = m_JdbcUtil.connect(driver, url,sUserName,sPassword);

        if(iRetCode != IP_SUCCESS) 
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipConnect():Error in connect\n");
            if(m_JdbcUtil.getLastException() != null)
		jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
	    else
		jdam.dam_addError(dam_hdbc, 0, 1, 0, "Error connecting to the Jdbc Driver");
        }

            return iRetCode;
        }

    public int ipDisconnect(long dam_hdbc)
    {
        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipDisonnect called\n");

        /* disconnect from the data source */
        if(m_JdbcUtil != null)
        {
            m_JdbcUtil.close();
            m_JdbcUtil = null;
        }

        return IP_SUCCESS;
    }

    public int ipStartTransaction(long dam_hdbc)
    {
            /* start a new transaction */
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipStartTransaction called\n");
            return IP_SUCCESS;
    }

    public int ipEndTransaction(long dam_hdbc,int iType)
    {
            /* end the transaction */
            jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipEndTransaction called\n");
            if (iType == DAM_COMMIT)
            {
            }
            else if (iType == DAM_ROLLBACK)
            {
            }
            return IP_SUCCESS;
    }

    public int ipExecute(long dam_hstmt, int iStmtType, long hSearchCol,xo_long piNumResRows)
    {
        int             iRetCode;
        StringBuffer    sSqlString, strInfo;
        XXX_STMT_DA     pStmtDA = null;
        int             idx = -1;

        jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipExecute called\n");

        /* check if we are connected */
        if ((m_JdbcUtil != null) && (!m_JdbcUtil.isConnected())) 
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute() Connection was not successful. Cannot execute query.");
            jdam.dam_addError(0, dam_hstmt, 1, 0, "Connection was not successful. Cannot execute query.");
            return IP_FAILURE;
        }

        sSqlString = new StringBuffer(MAX_QUERY_LEN);
        strInfo = new StringBuffer(ip.DAM_MAX_ID_LEN+1);

        /* initialize the result */
        m_iNumResRows = 0;

        /* process the query based on the type */

        if (iStmtType == DAM_SELECT) 
        {
            long    hrow = 0;
            xo_int  piValue;

            pStmtDA = new XXX_STMT_DA();
            pStmtDA.dam_hstmt = dam_hstmt;
            pStmtDA.iType = iStmtType;

            try 
            {
                ip_format_query(jdam.damex_getQuery(dam_hstmt), sSqlString);
            } 
            catch(Exception e)
            { 
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute(): Exception in formatting. " + e + "\n"); 
            }

            jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "m_bUseOriginalSelectList:" + m_bUseOriginalSelectList + "\n");
            jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Query:< " + sSqlString + ">\n");

            /* check if operating in Format-Only mode */
            if(m_JdbcUtil == null)
                return IP_SUCCESS;

	    if(m_bUseBulkFetch)
	    {
		pStmtDA.m_ResultBuffer = jdam.damex_allocResultBuffer(dam_hstmt);
		pStmtDA.m_ResultBuffer.setColumnType(DAM_COL_IN_USE);
	    }
            /* save the statement handle */
            idx = getStmtIndex();
            if(idx >= 0) 
            {
                m_stmtDA[idx] = pStmtDA;
            }

            jdam.dam_setIP_hstmt(dam_hstmt, idx); /* save the StmtDA index*/

            /* get fetch block size */
            piValue = new xo_int();
            iRetCode = jdam.dam_getInfo(0, pStmtDA.dam_hstmt, DAM_INFO_FETCH_BLOCK_SIZE,
                strInfo, piValue);
            if (iRetCode != DAM_SUCCESS)
                pStmtDA.iFetchSize = 2;
            else
                pStmtDA.iFetchSize = piValue.getVal();

            jdam.trace(m_tmHandle, UL_TM_INFO,"ipExecute() Fetch size = "+ pStmtDA.iFetchSize + "\n");

            boolean select = m_JdbcUtil.execute(sSqlString.toString());
            if(select)
                pStmtDA.rs = m_JdbcUtil.getResultSet();
            if (pStmtDA.rs == null) 
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute() Error executing query. m_JdbcUtil.execute() returned null resultset.\n");
                if(m_JdbcUtil.getLastException() == null)
                    jdam.dam_addError(0, dam_hstmt, 1, 0, "Error executing query. JdbcUtil.execute() returned null resultset.");
                else
                    jdam.dam_addError(0, dam_hstmt, 1, 0, m_JdbcUtil.getLastException().toString());
                return IP_FAILURE;
            }
            pStmtDA.iColCount = m_JdbcUtil.getColumnCount(pStmtDA.rs);
        }
        else  if ((iStmtType == DAM_INSERT) || (iStmtType == DAM_UPDATE) || (iStmtType == DAM_DELETE)) 
        {
            int iNumRows = 0;

            pStmtDA = new XXX_STMT_DA();
            pStmtDA.dam_hstmt = dam_hstmt;
            pStmtDA.iType = iStmtType;

            try 
            {
                ip_format_query(jdam.damex_getQuery(dam_hstmt), sSqlString);
            } 
            catch(Exception e)
            { 
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute():Exception in formatting. " + e + "\n"); 
            }

            String str = "Query:< " + sSqlString + ">\n";
            jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, str);

            /* check if operating in Format-Only mode */
            if(m_JdbcUtil == null)
                return IP_SUCCESS;

            boolean select = m_JdbcUtil.execute(sSqlString.toString());
            if(!select)
            {
                iNumRows = m_JdbcUtil.getUpdateCount();
                m_JdbcUtil.commit();
            }
            piNumResRows.setVal(iNumRows);
            m_JdbcUtil.closeStmt();
            pStmtDA = null;
            return IP_SUCCESS;
        }
        else if (iStmtType == DAM_FETCH) 
        { /* return more results from previously executed query */
            idx = -1;

            idx = (int)jdam.dam_getIP_hstmt(dam_hstmt);
            pStmtDA = m_stmtDA[idx];
        }
        else if (iStmtType == DAM_CLOSE) 
        { /* close results of executed query. Free any results */
            idx = (int)jdam.dam_getIP_hstmt(dam_hstmt);
            m_JdbcUtil.closeStmt();
	    pStmtDA = m_stmtDA[idx];
            if(m_bUseBulkFetch)
			jdam.damex_freeResultBuffer(dam_hstmt,pStmtDA.m_ResultBuffer);

            pStmtDA = null;
            m_stmtDA[idx] = null;
            return IP_SUCCESS;
        }
        else 
        {
            return IP_FAILURE;
        }

        if(m_bUseBulkFetch)
        {
            pStmtDA.iRowCount = 0;
            iRetCode = ip_process_buffer_rows(pStmtDA, DAM_SELECT == iStmtType ? true : false);
            piNumResRows.setVal(pStmtDA.iRowCount);
            return iRetCode;

        }
        /* get results from the query processor and build the results */

        pStmtDA.iRowCount = 0;
        try 
        {
            while (pStmtDA.iRowCount < pStmtDA.iFetchSize) 
            {
                if (!pStmtDA.rs.next()) 
                {
                    m_JdbcUtil.closeStmt();
                    m_stmtDA[idx] = null;
                    piNumResRows.setVal(pStmtDA.iRowCount);
                    return IP_SUCCESS;
                }

                iRetCode = ip_process_row(pStmtDA);
                if (iRetCode != DAM_SUCCESS) return iRetCode;
            }
            piNumResRows.setVal(pStmtDA.iRowCount);
        }
        catch(Exception e)
        { 
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "ipExecute(): " + e + "\n"); 
        }

        jdam.trace(m_tmHandle, UL_TM_INFO,"ip_execute() Query executed successfully\n");

        return DAM_SUCCESS_WITH_RESULT_PENDING;
    }

    public int ipSchema(long dam_hdbc,long pMemTree,int iType, long pList, Object pSearchObj)
    {
	jdam.trace(m_tmHandle, UL_TM_INFO,"ipSchema() Called\n");

	/* check if operating in Format-Only mode */
         if(m_JdbcUtil !=  null)
	    return ipSchemaBackEnd(dam_hdbc,pMemTree,iType,pList,pSearchObj);
	else
	    return ipSchemaFixed(dam_hdbc,pMemTree,iType,pList,pSearchObj);
    }

    public int ipSchemaFixed(long dam_hdbc,long pMemTree,int iType, long pList, Object pSearchObj)
    {
        switch(iType)
        {
        case DAMOBJ_TYPE_CATALOG:
            {
                schemaobj_table TableObj = new schemaobj_table(OA_CATALOG_NAME,null,null,null,null,null,null,null);

                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
            }
            break;
        case DAMOBJ_TYPE_SCHEMA:
            {
                schemaobj_table TableObj = new schemaobj_table();

                TableObj.SetObjInfo(null,"SYSTEM",null,null,null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

                TableObj.SetObjInfo(null,OA_USER_NAME,null,null,null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
            }
            break;
        case DAMOBJ_TYPE_TABLETYPE:
            {
                schemaobj_table TableObj = new schemaobj_table();

                TableObj.SetObjInfo(null,null,null,"SYSTEM TABLE",null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

                TableObj.SetObjInfo(null,null,null,"TABLE",null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

                TableObj.SetObjInfo(null,null,null,"VIEW",null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
            }

            break;
        case DAMOBJ_TYPE_TABLE:
            {
                schemaobj_table TableObj = new schemaobj_table();

                TableObj.SetObjInfo("SCHEMA","OAUSER","EMP","TABLE",null,null,null,"Employee Table");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj, TableObj);

                TableObj.SetObjInfo("SCHEMA","OAUSER","DEPT","TABLE",null,null,null,"Dept Table");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj, TableObj);

                TableObj.SetObjInfo("SCHEMA","OAUSER","DIVISION","TABLE",null,null,null,"Division Table");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj, TableObj);

                TableObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","TABLE",null,null,null,"Types Table");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
            }
            break;
        case DAMOBJ_TYPE_COLUMN:
            {
                /* DIVION TABLE COLUMNS */

                schemaobj_column ColumnObj = new schemaobj_column();


                ColumnObj.SetObjInfo("SCHEMA","OAUSER","DIVISION", "DIVNO",(short)4,"INTEGER",4,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Division Number");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","DIVISION", "DIVISION",(short)12,"VARCHAR",255,255,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NO_NULLS,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Division name");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);


                /* DEPT TABLE COLUMNS */

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","DEPT","DEPTNO",(short)4,"INTEGER",4,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Dept Number");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","DEPT","DNAME",(short)12,"VARCHAR",255,255,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NO_NULLS,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Dept name");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","DEPT","LOC",(short)12,"VARCHAR",255,255,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NO_NULLS,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Location");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","DEPT","DIVNO",(short)4,"INTEGER",4,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Division number");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                /* EMP TABLE COLUMNS */

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","EMPNO",(short)4,"INTEGER",4,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NO_NULLS,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Empid Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","ENAME",(short)12,"VARCHAR",255,255,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Ename");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","JOB",(short)12,"VARCHAR",255,255,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Ename");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","HIREDATE",(short)11,"TIMESTAMP",16,19,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Date Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","SAL",(short)6,"FLOAT",8,15,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Salary");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","COMM",(short)6,"FLOAT",8,15,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Commission");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","EMP","DEPTNO",(short)4,"INTEGER",4,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)SQL_PC_NOT_PSEUDO,(short)0,"Dept Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                // Types Table columns
                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","ID",(short)4,"INTEGER",4,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"ID/Integer Field");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","CHAR_VAL",(short)1,"CHAR",16,16,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Character Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","NUMERIC_VAL",(short)2,"NUMERIC",34,32,(short)DAMOBJ_NOTSET,(short)5,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Numeric Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","REAL_VAL",(short)7,"REAL",4,7,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Real Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","DOUBLE_VAL",(short)8,"DOUBLE",8,15,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Double Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","DATE_VAL",(short)9,"DATE",6,10,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Date Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","TIME_VAL",(short)10,"TIME",6,8,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Date Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","TIMESTAMP_VAL",(short)11,"TIMESTAMP",16,19,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Date Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","VARCHAR_VAL",(short)12,"CHAR",32,32,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Varchar Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","BIT_VAL",(short)XO_TYPE_BIT,"BIT",1,1,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Bit Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);

                ColumnObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE","TINYINT_VAL",(short)XO_TYPE_TINYINT,"TINYINT",1,3,(short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
                    (short)XO_NULLABLE,(short)DAMOBJ_NOTSET,null,null,(short)DAMOBJ_NOTSET,(short)0,"Tinyint Value");
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);


            }
            break;

        case DAMOBJ_TYPE_STAT:
            {
                schemaobj_stat StatObj = new schemaobj_stat("SCHEMA","OAUSER","EMP",(short)1,null,"EMPNO",(short)3,(short)1,"EMPNO",
                                                            "A",DAMOBJ_NOTSET,DAMOBJ_NOTSET,null);

                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,StatObj);

                StatObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE",(short)1,null,"ID_INDEX",(short)1,(short)1,"ID",
                        "A",DAMOBJ_NOTSET,DAMOBJ_NOTSET,null);
                 jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,StatObj);

                StatObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE",(short)1,null,"BIT_VAL_INDEX",(short)1,(short)1,"BIT_VAL",
                         "A",DAMOBJ_NOTSET,DAMOBJ_NOTSET,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,StatObj);

                StatObj.SetObjInfo("SCHEMA","OAUSER","TYPES_TABLE",(short)1,null,"TINYINT_VAL_INDEX",(short)1,(short)1,"TINYINT_VAL",
                        "A",DAMOBJ_NOTSET,DAMOBJ_NOTSET,null);
               jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,StatObj);
            }
            break;

        case DAMOBJ_TYPE_FKEY:
            break;
        case DAMOBJ_TYPE_PROC:
	    {
                schemaobj_proc ProcObj = new schemaobj_proc();
                schemaobj_proc pSearchProcObj = null;
                pSearchProcObj = (schemaobj_proc)pSearchObj;

                if (pSearchProcObj != null)
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for Procedure:<"+pSearchProcObj.getQualifier()+"."+pSearchProcObj.getOwner()+"."+pSearchProcObj.getProcName()+"> is being requested\n");            
                }
                else
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for all Procedures is being requested\n");
                }

		ProcObj.SetObjInfo("SCHEMA","OAUSER", "MULTIRESULT_REGULAR", (int)1, (int)DAMOBJ_NOTSET, (int)1, 
                                    (short)DAMOBJ_NOTSET, null, "Returns multiple results");
                jdam.dam_add_schemaobj(pMemTree,iType, pList, pSearchObj, ProcObj);
            }
            break;
	case DAMOBJ_TYPE_PROC_COLUMN:
	    {
		schemaobj_proccolumn ProcColumnObj = new schemaobj_proccolumn();

                schemaobj_proccolumn pSearchProcColumnObj = null;
                pSearchProcColumnObj = (schemaobj_proccolumn)pSearchObj;
                
                if (pSearchProcColumnObj != null)
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for column <"+pSearchProcColumnObj.getColumnName()+"> of Procedure:<"+pSearchProcColumnObj.getQualifier()+"."+pSearchProcColumnObj.getOwner()+"."+pSearchProcColumnObj.getProcName()+"> is being requested\n");
                }
                else
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for all columns of all Procedures is being requested\n");
                }
		{ /* MULTIRESULT_REGULAR */
		    /* Add Proc column definitions */
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","ITEMS",
			(short)SQL_PARAM_INPUT, (short)4,"INTEGER", 10, 4, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NULLABLE,null,"Number of result rows to Return");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
    
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","NAME",
			(short)SQL_RESULT_COL, (short)12,"VARCHAR", 255, 255, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NO_NULLS,null,"Name of the Employee");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
    
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","ID",
			(short)SQL_RESULT_COL, (short)4,"INTEGER", 10, 4, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NULLABLE,null,"ID of the employee");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
    
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","RETVAL",
			(short)SQL_RETURN_VALUE, (short)4,"INTEGER", 10, 4, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NULLABLE,null,"Number of result rows to Return");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
                } /* MULTIRESULT_REGULAR */
	    }
            break;
        default:
            break;
        }

       return IP_SUCCESS;

    }

    public int ipSchemaBackEnd(long dam_hdbc,long pMemTree,int iType, long pList, Object pSearchObj)
    {
	jdam.trace(m_tmHandle, ip.UL_TM_INFO, "ipSchemaBackEnd() called.Statement type:<"+ iType + ">\n");
	int rc=0;

        switch(iType)
        {
        case DAMOBJ_TYPE_CATALOG:
            {
                rc = generateCatalogList(pMemTree,iType,pList,pSearchObj);
	       if(rc != IP_SUCCESS)
	       {
		   if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
	       }
		   
            }
            break;
        case DAMOBJ_TYPE_SCHEMA:
            {
                rc = generateSchemasList(pMemTree,iType,pList,pSearchObj);
	       if(rc != IP_SUCCESS)
	       {
		   if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
	       }
		   
            }
            break;
        case DAMOBJ_TYPE_TABLETYPE:
            {
                schemaobj_table TableObj = new schemaobj_table();

                TableObj.SetObjInfo(null,null,null,"SYSTEM TABLE",null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

                TableObj.SetObjInfo(null,null,null,"TABLE",null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);

                TableObj.SetObjInfo(null,null,null,"VIEW",null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,TableObj);
            }

            break;
        case DAMOBJ_TYPE_TABLE:
            {
               rc = generateTableList(pMemTree,iType,pList,pSearchObj);
	       if(rc != IP_SUCCESS)
	       {
		   if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
	       }

            }
            break;
        case DAMOBJ_TYPE_COLUMN:
            {
		rc = generateColumnList(pMemTree,iType,pList,pSearchObj);
		if(rc != IP_SUCCESS)
		{
		    if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
		}
            }
            break;

        case DAMOBJ_TYPE_STAT:
                {
                rc = generateStatistics(pMemTree,iType,pList,pSearchObj);
                if(rc != IP_SUCCESS)
		{
		    if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
		}
                    
            }
                break;
            case DAMOBJ_TYPE_PKEY:
                {
                rc = generatePrimaryKeys(pMemTree,iType,pList,pSearchObj);
                if(rc != IP_SUCCESS)
		{
		    if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
		}
            }
                break;
            case DAMOBJ_TYPE_FKEY:
                {
                rc = generateForeignKeys(pMemTree,iType,pList,pSearchObj);
                if(rc != IP_SUCCESS)
		{
		    if(m_JdbcUtil.getLastException() != null)
		       jdam.dam_addError(dam_hdbc, 0, 1, 0, m_JdbcUtil.getLastException().toString());
		   return rc;
		}
            }
            break;
	case DAMOBJ_TYPE_PROC:
	    {
                schemaobj_proc ProcObj = new schemaobj_proc();
                schemaobj_proc pSearchProcObj = null;
                pSearchProcObj = (schemaobj_proc)pSearchObj;

                if (pSearchProcObj != null)
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for Procedure:<"+pSearchProcObj.getQualifier()+"."+pSearchProcObj.getOwner()+"."+pSearchProcObj.getProcName()+"> is being requested\n");            
                }
                else
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for all Procedures is being requested\n");
                }

		ProcObj.SetObjInfo("SCHEMA","OAUSER", "MULTIRESULT_REGULAR", (int)1, (int)DAMOBJ_NOTSET, (int)1, 
                                    (short)DAMOBJ_NOTSET, null, "Returns multiple results");
                jdam.dam_add_schemaobj(pMemTree,iType, pList, pSearchObj, ProcObj);
            }
            break;
	case DAMOBJ_TYPE_PROC_COLUMN:
	    {
		schemaobj_proccolumn ProcColumnObj = new schemaobj_proccolumn();

                schemaobj_proccolumn pSearchProcColumnObj = null;
                pSearchProcColumnObj = (schemaobj_proccolumn)pSearchObj;
                
                if (pSearchProcColumnObj != null)
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for column <"+pSearchProcColumnObj.getColumnName()+"> of Procedure:<"+pSearchProcColumnObj.getQualifier()+"."+pSearchProcColumnObj.getOwner()+"."+pSearchProcColumnObj.getProcName()+"> is being requested\n");
                }
                else
                {
                    jdam.trace(m_tmHandle, UL_TM_MAJOR_EV, "Schema for all columns of all Procedures is being requested\n");
                }
		{ /* MULTIRESULT_REGULAR */
		    /* Add Proc column definitions */
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","ITEMS",
			(short)SQL_PARAM_INPUT, (short)4,"INTEGER", 10, 4, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NULLABLE,null,"Number of result rows to Return");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
    
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","NAME",
			(short)SQL_RESULT_COL, (short)12,"VARCHAR", 255, 255, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NO_NULLS,null,"Name of the Employee");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
    
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","ID",
			(short)SQL_RESULT_COL, (short)4,"INTEGER", 10, 4, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NULLABLE,null,"ID of the employee");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
    
		    ProcColumnObj.SetObjInfo("SCHEMA","OAUSER","MULTIRESULT_REGULAR","RETVAL",
			(short)SQL_RETURN_VALUE, (short)4,"INTEGER", 10, 4, (short)DAMOBJ_NOTSET,(short)DAMOBJ_NOTSET,
			(short)XO_NULLABLE,null,"Number of result rows to Return");
		    jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ProcColumnObj);
                } /* MULTIRESULT_REGULAR */
	    }
            break;
        default:
            break;
        }

       return IP_SUCCESS;

    }

    /* this example uses dynamic schema and only SELECT command supported, so following functions are not called */
    public int        ipDDL(long dam_hstmt, int iStmtType,xo_long piNumResRows)
    {
                return IP_FAILURE;
    }

    public int        ipProcedure(long dam_hstmt, int iType, xo_long piNumResRows)
    {
	StringBuffer    sProcName;
        long            hrow, hRowElem;
        int             iParamNum;
        long            hcol;
        StringBuffer    sColName;
        xo_int          piXOType, piColumnType;
        xo_int          iValueStatus;

        int             iItems;
        int             iElements;
        int             iRetVal = 0;
        int             iRetCode;
        boolean         bEmpProcedure = false;
        boolean         bCheckStatusInt = false;
        boolean         bCheckStatusChar = false;
        boolean         bProcOutRet = false;
        boolean         bArrayProcedure = false;


        sProcName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);


        /* get the procedure information */
        jdam.dam_describeProcedure(dam_hstmt, null, null, sProcName,null);

	jdam.trace(m_tmHandle, UL_TM_F_TRACE,"ipProcedure called. Procedure Name <"+ sProcName +">\n");
        if (!sProcName.toString().equalsIgnoreCase("MULTIRESULT_REGULAR"))
        {
	    jdam.dam_addError(0, dam_hstmt, DAM_IP_ERROR, 0, "Queries on Stored Procedure are not supported.");
	    return DAM_FAILURE;
        }

	return passjdbc_procedure_multiresult_regular(dam_hstmt, iType, piNumResRows);
    }

    public int        ipDCL(long dam_hstmt, int iStmtType,xo_long piNumResRows)
    {
                return IP_FAILURE;
    }

    public int        ipPrivilege(int iStmtType,String pcUserName,String pcCatalog,String pcSchema,String pcObjName)
    {
                return IP_FAILURE;
    }

    public int        ipNative(long dam_hstmt, int iCommandOption, String sCommand, xo_long piNumResRows)
    {
                return IP_FAILURE;
    }

    public int        ipSchemaEx(long dam_hstmt, long pMemTree, int iType, long pList,Object pSearchObj)
    {
                return IP_FAILURE;
    }

    public int        ipProcedureDynamic(long dam_hstmt, int iType, xo_long piNumResRows)
    {
                return IP_FAILURE;
    }

    /********************************************************************************************               
	Method:          passjdbc_procedure_multiresult_regular()
    *********************************************************************************************/
	public int        passjdbc_procedure_multiresult_regular(long dam_hstmt, int iType, xo_long piNumResRows)
	{
	    PASSJDBC_PROC_DA  pProcDA;
	    int               iRetCode;
	    int             idx;

	    jdam.trace(m_tmHandle, UL_TM_F_TRACE,"passjdbc_procedure_multiresult_regular called\n");
        if (iType == DAM_PROCEDURE) 
        {
            long     pMemTree;

            pMemTree = jdam.dam_getMemTree(dam_hstmt);
            pProcDA = new PASSJDBC_PROC_DA();
            pProcDA.dam_hstmt = dam_hstmt;
            pProcDA.pMemTree = pMemTree;
            jdam.dam_describeProcedure(dam_hstmt, pProcDA.sQualifier, pProcDA.sOwner, pProcDA.sProcName, pProcDA.sUserData);

            /* get the statement handle */
            idx = getProcIndex();
            if(idx >= 0) 
            {
                procDA[idx] = pProcDA;
            }

            jdam.dam_setIP_hstmt(dam_hstmt, idx); /* save the StmtDA index*/
            pProcDA.iItems = 0;

            /* get the fetch block size for cursor mode */
            {
                xo_int  piValue;

                piValue = new xo_int();
                /* get fetch block size */
                iRetCode = jdam.dam_getInfo(0, pProcDA.dam_hstmt, DAM_INFO_FETCH_BLOCK_SIZE,
                    null, piValue);

                if (iRetCode != DAM_SUCCESS)
                    pProcDA.iFetchSize = 2;
                else
                    pProcDA.iFetchSize = piValue.getVal();

                jdam.trace(m_tmHandle, UL_TM_INFO,"mem_procedure_multiresult() Fetch size = "+ pProcDA.iFetchSize + "\n");
            }

            /* get the input parameter info */
            {
                long            hrow, hRowElem;
                long            hcol;
                StringBuffer    sColName;
                xo_int          piXOType, piColumnType;
                xo_int          iValueStatus;

                sColName = new StringBuffer(ip.DAM_MAX_ID_LEN + 1);
                piXOType = new xo_int(0);
                piColumnType = new xo_int(0);
                iValueStatus = new xo_int(0);

                hrow = jdam.dam_getInputRow(dam_hstmt);

                /* Get and print the value of the input parameters */
                for (hRowElem = jdam.dam_getFirstValueSet(dam_hstmt, hrow); hRowElem != 0;
                    hRowElem = jdam.dam_getNextValueSet(dam_hstmt)) 
                {

                    String  sVal;

                    hcol = jdam.dam_getColToSet(hRowElem);   
                    jdam.dam_describeCol(hcol, null, null, piXOType, null);
                    jdam.dam_describeColDetail(hcol, null, piColumnType, null);
                    if (piColumnType.getVal() != SQL_PARAM_INPUT) continue;

                    sVal = (String) jdam.dam_getValueToSet(hRowElem, XO_TYPE_CHAR, iValueStatus);
                    pProcDA.iItems =new Integer(sVal).intValue();
                }
            }
        }

        if (iType == DAM_PROCEDURE || iType == DAM_FETCH) 
        {
            long        hColName, hColId, hColD;
            String      sName, sColName;
            int         lVal;
            int         iRowCount, iColCount;
            long        hRow;

            idx = (int)jdam.dam_getIP_hstmt(dam_hstmt);
            pProcDA = procDA[idx];

            if (iType == DAM_PROCEDURE)
                pProcDA.iCurItems = 0;

            hColName = jdam.dam_getCol(dam_hstmt, "NAME");
            hColId = jdam.dam_getCol(dam_hstmt, "ID");

            /* build result set */
            iRowCount = 0;
            while (pProcDA.iCurItems < pProcDA.iItems) 
            {
                hRow = jdam.dam_allocRow(dam_hstmt);

                sName = "Name" + "-" + pProcDA.iCurItems;
                lVal = pProcDA.iCurItems;
                iRetCode = jdam.dam_addCharValToRow(dam_hstmt, hRow, hColName, sName, XO_NTS);
                iRetCode = jdam.dam_addIntValToRow(dam_hstmt, hRow, hColId , lVal, 0);
                jdam.dam_addRowToTable(dam_hstmt, hRow);
                pProcDA.iCurItems++;
                iRowCount++;
                if (iRowCount >= pProcDA.iFetchSize) 
                {     
                    return DAM_SUCCESS_WITH_RESULT_PENDING;
                }
            }
            {
                long        hcolRetVal;
                long        hOutputRow;

                hcolRetVal = jdam.dam_getCol(dam_hstmt, "RETVAL");           

                /* build output row */
                hOutputRow = jdam.dam_allocOutputRow(dam_hstmt);
                lVal = pProcDA.iItems;
                iRetCode = jdam.dam_addIntValToRow(dam_hstmt, hOutputRow, hcolRetVal , lVal, 0);            
                if (iRetCode != DAM_SUCCESS) return iRetCode;           

                iRetCode = jdam.dam_addOutputRow(dam_hstmt, hOutputRow);
                if (iRetCode != DAM_SUCCESS) return iRetCode;
            }
        }
        else if ( iType == DAM_CLOSE )
        {
            idx = (int)jdam.dam_getIP_hstmt(dam_hstmt);
            pProcDA = procDA[idx];                
        }
        else 
        {
            return IP_FAILURE;
        }

        /* when processing is fully done, release from Proc Array */
        if(idx >= 0) 
        {
            procDA[idx] = null;
        }
        jdam.dam_setIP_hstmt(dam_hstmt, 0);
        return IP_SUCCESS;
    }

/********************************************************************************************               
    Method:          getProcIndex()
    Description:     Return the index of the ProcDA to use

*********************************************************************************************/       
    public int getProcIndex()
    {
        for (int i=0; i < procDA.length; i++) 
        {
            if(procDA[i] == null)
                return i;
        }
        return -1;
    }
    // Utility functions
    private int ip_read_configuration()
    {
            String          sTemp = "";
            int             iTemp;

	        /* If 1, pushed to back-end and if 0, DAM handles Postprocessing */
            sTemp = jdam.getProfileString("PASSIP", "PushPostProcessing", "1", m_sIniFile.toString());
            iTemp = new Integer(sTemp).intValue();
            if (iTemp == 1)
                m_bPushPostProcessing = true;
            else
                m_bPushPostProcessing = false;

	    /* If 1, use bulk fetch if 0, normal mode */
            sTemp = jdam.getProfileString("PASSIP", "UseBulkFetch", "0", m_sIniFile.toString());
            iTemp = new Integer(sTemp).intValue();
            if (iTemp == 0)
                m_bUseBulkFetch = false;
            else
                m_bUseBulkFetch = true;
            /* Get IgnoreDateTimeParseError flag from ini */
            sTemp = jdam.getProfileString("PASSIP", "IgnoreDateTimeParseError", "0", m_sIniFile.toString());
            iTemp = new Integer(sTemp).intValue();
            if (iTemp == 1)
                m_bIgnoreDateTimeParseError = true;
            else
                m_bIgnoreDateTimeParseError = false;
			
			/* Get ValidateNullConstraint flag from ini */
			sTemp = jdam.getProfileString("PASSIP", "ValidateNullConstraint", "0", m_sIniFile.toString());
            iTemp = new Integer(sTemp).intValue();
            if (iTemp == 1)
                m_bValidateNullConstraint = true;
            else
                m_bValidateNullConstraint = false;

            return IP_SUCCESS;
    }


/************************************************************************
Function:       ip_format_query()
Description:
Return:
************************************************************************/
    int             ip_format_query(long hquery, StringBuffer pSqlBuffer)
    {
        int         iQueryType;

        iQueryType = jdam.damex_getQueryType(hquery);

        switch (iQueryType) 
        {
        case DAM_SELECT:
            ip_format_select_query(hquery, pSqlBuffer);
            break;
        case DAM_INSERT:
            ip_format_insert_query(hquery, pSqlBuffer);
            break;
        case DAM_UPDATE:
            ip_format_update_query(hquery, pSqlBuffer); 
            break;
        case DAM_DELETE:
            ip_format_delete_query(hquery, pSqlBuffer);
            break;
	case DAM_TABLE:
            ip_format_table_query(hquery,pSqlBuffer);
        default:
            break;
        }

        return IP_SUCCESS;
    }

/********************************************************************************************
    Method:         ip_format_select_query
    Description:    Format the given query
    Return:         IP_SUCCESS on success
                    IP_FAILURE on error
*********************************************************************************************/

    int ip_format_select_query(long hquery, StringBuffer pSqlBuffer)
    {
    xo_int            piSetQuantifier;
    xo_long           phSelectValExpList, phGroupValExpList, phOrderValExpList;
    xo_long           phSearchExp;
    xo_long           phHavingExp;
    xo_long           piTopResRows;
    xo_int            pbTopPercent;
    xo_int            piUnionType = new xo_int(0);
    xo_long           phUnionQuery = new xo_long(0);

    piSetQuantifier     = new xo_int(0);
    phSelectValExpList  = new xo_long(0);
    phGroupValExpList   = new xo_long(0);
    phOrderValExpList   = new xo_long(0);
    phSearchExp         = new xo_long(0);
    phHavingExp         = new xo_long(0);
    piTopResRows        = new xo_long(0);
    pbTopPercent        = new xo_int(0);

    try
    {
        /* LTR UNION processing - so check if query has a UNION clause and format it first */
        jdam.damex_describeUnionQuery(hquery, piUnionType, phUnionQuery);

        if (phUnionQuery.getVal() != 0) 
        {
            ip_format_select_query(phUnionQuery.getVal(), pSqlBuffer);
            pSqlBuffer.append(" UNION ");
            if (piUnionType.getVal() != 0)
                pSqlBuffer.append(" ALL ");
        }

    jdam.damex_describeSelectQuery(hquery, piSetQuantifier,
                                        phSelectValExpList,
                                        phSearchExp,
                                        phGroupValExpList,
                                        phHavingExp,
                                        phOrderValExpList);

    jdam.damex_describeSelectTopClause(hquery, piTopResRows, pbTopPercent);


    /* check if query cannot use orginal select expression */
            if (!m_bPushPostProcessing) 
            {
        m_bUseOriginalSelectList = ip_isOriginalSelectListCompatible(phSelectValExpList.getVal(),
                                        phGroupValExpList.getVal(), phOrderValExpList.getVal());
        }

    /* get the table list */
    {
    long        htable;

    StringBuffer pUserData = new StringBuffer(0);
    xo_int piColNum = new xo_int();
    xo_int piTablNum = new xo_int();
    xo_int piXoType = new xo_int();
    xo_int piColType = new xo_int();
    xo_int piResultColNum = new xo_int();
    htable = jdam.damex_getFirstTable(hquery);
                while (htable != 0) 
                {
                    xo_int      piTableNum = new xo_int(0);
                    StringBuffer wsTableName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
                    long    hCol;
                    StringBuffer wsColName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);

                    jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);
                    /*
                     System.out.print("Table:" + sTableName +". Columns Used: ", sTableName);
                     */
                    hCol = jdam.damex_getFirstCol(htable, DAM_COL_IN_USE);
                    while (hCol != 0) 
                    {
                        jdam.damex_describeCol(hCol, piTablNum, piColNum, wsColName, piXoType, piColType, pUserData, piResultColNum);
                        /*
                         System.out.print(sColName + ",");
                         */
                        hCol = jdam.damex_getNextCol(htable);
                    }
                    /*
                     System.out.println("");
                     */
                    htable = jdam.damex_getNextTable(hquery);
                }
            }

            pSqlBuffer.append("SELECT ");
            if(!m_bUseOriginalSelectList) 
            {
                ip_format_col_in_use(hquery, pSqlBuffer);
            }
            else 
            {
                if ((piSetQuantifier.getVal() == SQL_SELECT_DISTINCT) && (m_bPushPostProcessing))
                    pSqlBuffer.append("DISTINCT ");
                if (piTopResRows.getVal() != DAM_NOT_SET) 
                {
                    pSqlBuffer.append("TOP ").append(piTopResRows.getVal()).append(" ");
                    if (pbTopPercent.getVal() != 0)
                        pSqlBuffer.append("PERCENT ");
                }
                ip_format_valexp_list(hquery, phSelectValExpList.getVal(), pSqlBuffer);
            }

            pSqlBuffer.append(" FROM ");
            ip_format_table_list(hquery, pSqlBuffer);
            if (phSearchExp.getVal() != 0) 
            {
                pSqlBuffer.append("WHERE ");
                ip_format_logexp(hquery, phSearchExp.getVal(), pSqlBuffer);
            }
            if ((phGroupValExpList.getVal() != 0) && (m_bPushPostProcessing)) 
            {
                pSqlBuffer.append(" GROUP BY ");
                ip_format_group_list(hquery, phGroupValExpList.getVal(), pSqlBuffer);
            }
            if ((phHavingExp.getVal() != 0) && (m_bPushPostProcessing)) 
            {
                pSqlBuffer.append(" HAVING ");
                ip_format_logexp(hquery, phHavingExp.getVal(), pSqlBuffer);
            }

            if ((phOrderValExpList.getVal() != 0) && (m_bPushPostProcessing)) 
            {
                pSqlBuffer.append(" ORDER BY ");
                ip_format_order_list(hquery, phOrderValExpList.getVal(), pSqlBuffer);
            }

        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }

        return IP_SUCCESS;

    }

/********************************************************************************************
    Method:         ip_format_insert_query
    Description:    Format the given query
    Return:         IP_SUCCESS on success
                    IP_FAILURE on error
*********************************************************************************************/

    int 	ip_format_insert_query(long hquery, StringBuffer pSqlBuffer)
    {
	xo_long 	phTable, phColList,phInputRowList, phInsertQuery;
        StringBuffer    wsCatalog   = new StringBuffer(DAM_MAX_ID_LEN+1);
        StringBuffer    wsSchema    = new StringBuffer(DAM_MAX_ID_LEN+1);
	StringBuffer    wsTableName = new StringBuffer(DAM_MAX_ID_LEN+1);

	phTable		= new xo_long(0);
	phColList	= new xo_long(0);
	phInputRowList	= new xo_long(0);
	phInsertQuery	= new xo_long(0);

        try
        {
            jdam.damex_describeInsertQuery(hquery, phTable, phColList,phInputRowList, phInsertQuery);
	    pSqlBuffer.append("INSERT INTO ");
            jdam.damex_describeTable(phTable.getVal(), null, wsCatalog, wsSchema, wsTableName, null, null);

	    if(wsCatalog != null && wsCatalog.length() > 0)
                pSqlBuffer.append(wsCatalog).append(".");
            if(wsSchema != null && wsSchema.length() > 0)
                pSqlBuffer.append(wsSchema).append(".");
	    pSqlBuffer.append(wsTableName).append(" ");
            /* columns */
	    {
		long    	hCol;
		int        	iFirst;

		pSqlBuffer.append("( ");

		iFirst = TRUE;
		hCol = jdam.damex_getFirstColInList(phColList.getVal());

                while (hCol != 0) 
                {
                    StringBuffer wsColName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);

                    if(iFirst == FALSE)
                        pSqlBuffer.append(", ");
                    else
                        iFirst = FALSE;
        
                    jdam.damex_describeCol(hCol,null,null,wsColName, null, null, null, null);

                    pSqlBuffer.append(wsColName);
                    hCol = jdam.damex_getNextColInList(phColList.getVal());
                }
                pSqlBuffer.append(" ) ");
            }
            if(phInputRowList.getVal() != 0)
            {
                pSqlBuffer.append("VALUES ");
                ip_format_insertrow_list(hquery, phInputRowList.getVal(), pSqlBuffer);
            }
            else
            {
                ip_format_query(phInsertQuery.getVal(), pSqlBuffer);
            }

        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }
        return IP_SUCCESS;
    }

    int     ip_format_insertrow_list(long hquery, long hRowList, StringBuffer pSqlBuffer)
    {
        try
        {
            long    hRow;
            int     iFirst = TRUE;
            int     iMultiRowInsert = FALSE;

            hRow = jdam.damex_getFirstInsertRow(hRowList);
            if (jdam.damex_getNextInsertRow(hRowList) != 0) iMultiRowInsert = TRUE;

            if (iMultiRowInsert == TRUE)
                pSqlBuffer.append("( ");

            hRow = jdam.damex_getFirstInsertRow(hRowList);
            while (hRow != 0) 
            {
                if (iFirst == FALSE)
                    pSqlBuffer.append(", ");
                else
                    iFirst = FALSE;
                ip_format_insertrow(hquery, hRow, pSqlBuffer);

                hRow = jdam.damex_getNextInsertRow(hRowList);
            }

            if (iMultiRowInsert == TRUE)
                pSqlBuffer.append("  )");
        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }
        return IP_SUCCESS;
    }

    int     ip_format_insertrow(long hquery,long hRow,StringBuffer pSqlBuffer)
    {
        try
        {
            long    hValExp;
            int     iFirst = TRUE;

            pSqlBuffer.append("( ");

            hValExp = jdam.damex_getFirstInsertValueExp(hquery, hRow);
            while (hValExp != 0) 
            {
                if (iFirst == FALSE)
                    pSqlBuffer.append(", ");
                else
                    iFirst = FALSE;

                ip_format_valexp(hquery, hValExp, pSqlBuffer);

                hValExp = jdam.damex_getNextInsertValueExp(hquery);
            }
            pSqlBuffer.append(" )");
        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }
        return IP_SUCCESS;
    }

/********************************************************************************************
    Method:         ip_format_update_query
    Description:    Format the given query
    Return:         IP_SUCCESS on success
                    IP_FAILURE on error
*********************************************************************************************/

    int ip_format_update_query(long hquery, StringBuffer pSqlBuffer)
    {
        try
        {
            xo_long     phTable;
            xo_long     phRow;
            xo_long     phSearchExp;
            xo_int              piTableNum;
            StringBuffer    wsTableName = new StringBuffer(DAM_MAX_ID_LEN+1);
            StringBuffer    wsCatalog   = new StringBuffer(DAM_MAX_ID_LEN+1);
            StringBuffer    wsSchema    = new StringBuffer(DAM_MAX_ID_LEN+1);

            phTable         = new xo_long(0);
            phRow       = new xo_long(0);
            phSearchExp     = new xo_long(0);
            piTableNum      = new xo_int(0);

            jdam.damex_describeUpdateQuery(hquery, phTable, phRow, phSearchExp);

            pSqlBuffer.append("UPDATE ");

            jdam.damex_describeTable(phTable.getVal(), piTableNum, wsCatalog, wsSchema, wsTableName, null, null);

	    if(wsCatalog != null && wsCatalog.length() > 0)
                pSqlBuffer.append(wsCatalog).append(".");
            if(wsSchema != null && wsSchema.length() > 0)
                pSqlBuffer.append(wsSchema).append(".");
            pSqlBuffer.append(wsTableName).append(" T").append(piTableNum.getVal()).append("_Q").append(hquery).append(" ");

            ip_format_update_list(hquery, phRow.getVal(), pSqlBuffer);

            if (phSearchExp.getVal() != 0) 
            {
                pSqlBuffer.append(" WHERE ");
                ip_format_logexp(hquery, phSearchExp.getVal(), pSqlBuffer);
            }
        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }
        return IP_SUCCESS;
    }

    int     ip_format_update_list(long hquery, long hRow, StringBuffer pSqlBuffer)
    {
        try
        {
            xo_long     phCol;
            long        hValExp;
            int         iFirst;

            phCol       = new xo_long(0);
            iFirst = TRUE;

            pSqlBuffer.append("SET ");
            hValExp = jdam.damex_getFirstUpdateSet(hquery, hRow, phCol);

            while (hValExp != 0) 
            {
                if (iFirst == FALSE)
                    pSqlBuffer.append(", ");
                else
                    iFirst = FALSE;

                ip_format_col(hquery, phCol.getVal(), pSqlBuffer);
                pSqlBuffer.append(" = ");
                ip_format_valexp(hquery, hValExp, pSqlBuffer);
                hValExp = jdam.damex_getNextUpdateSet(hquery, phCol);
            }
        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }
        return IP_SUCCESS;
    }

/********************************************************************************************
    Method:         ip_format_delete_query
    Description:    Format the given query
    Return:         IP_SUCCESS on success
                    IP_FAILURE on error
*********************************************************************************************/

    int ip_format_delete_query(long hquery, StringBuffer pSqlBuffer)
    {
        try
        {
            xo_long     phTable;
            xo_long     phSearchExp;
            StringBuffer    wsCatalog   = new StringBuffer(DAM_MAX_ID_LEN+1);
            StringBuffer    wsSchema    = new StringBuffer(DAM_MAX_ID_LEN+1);
            StringBuffer    wsTableName = new StringBuffer(DAM_MAX_ID_LEN+1);

            phTable     = new xo_long(0);
            phSearchExp     = new xo_long(0);

            jdam.damex_describeDeleteQuery(hquery, phTable, phSearchExp);

            pSqlBuffer.append("DELETE FROM ");

            jdam.damex_describeTable(phTable.getVal(), null, wsCatalog, wsSchema, wsTableName, null, null);

	    if(wsCatalog != null && wsCatalog.length() > 0)
                pSqlBuffer.append(wsCatalog).append(".");
            if(wsSchema != null && wsSchema.length() > 0)
                pSqlBuffer.append(wsSchema).append(".");

            pSqlBuffer.append(wsTableName).append(" ");

            if (phSearchExp.getVal() != 0) 
            {
                pSqlBuffer.append("WHERE ");
                ip_format_logexp(hquery, phSearchExp.getVal(), pSqlBuffer);
            }
        }
        catch(Exception e)
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "Error: " + e + "\n");
        }
        return IP_SUCCESS;
    }

    int     ip_format_col_in_use(long hquery, StringBuffer pSqlBuffer)
    {
        long       htable;
        int        iFirst = TRUE;

        htable = jdam.damex_getFirstTable(hquery);
        while (htable != 0) 
        {
            xo_int      piTableNum = new xo_int(0);
            StringBuffer wsTableName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
            long    hCol;

            jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);
            hCol = jdam.damex_getFirstCol(htable, DAM_COL_IN_USE);

            while (hCol != 0) 
            {
                if(iFirst == FALSE)
                    pSqlBuffer.append(", ");
                ip_format_col(hquery, hCol, pSqlBuffer);
                hCol = jdam.damex_getNextCol(htable);
                iFirst = FALSE;
            }
            htable = jdam.damex_getNextTable(hquery);
        }
        return IP_SUCCESS;
    }

    int     ip_format_col(long hquery, long hCol, StringBuffer pSqlBuffer)
    {
		xo_int piTableNum, piNestedTableNum, piColNum;
        StringBuffer wsColName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
        long         hParentQuery;

        piTableNum  = new xo_int(0);
		piNestedTableNum = new xo_int(0);
        piColNum    = new xo_int(0);

		jdam.damex_describeColEx(hCol, piTableNum, piNestedTableNum, piColNum,
				wsColName, null, null, null, null);
		if (piNestedTableNum.getVal() != DAM_NOT_SET)
			piTableNum.setVal(piNestedTableNum.getVal());

        /* check if TableNum is valid. For COUNT(*) the column has iTableNum not set */
        if (piTableNum.getVal() == DAM_NOT_SET)
            return IP_SUCCESS;

        if(jdam.damex_getQueryType(hquery) == DAM_SELECT)
        {
            if (jdam.damex_isCorrelatedCol(hCol) != 0) 
            {
                hParentQuery = jdam.damex_getParentQuery(hquery);
                pSqlBuffer.append("T").append(piTableNum.getVal()).append("_Q").append(hParentQuery).append(".\"").append(wsColName).append("\"");
            }
            else
                pSqlBuffer.append("T").append(piTableNum.getVal()).append("_Q").append(hquery).append(".\"").append(wsColName).append("\"");;
        }
        else 
        {
            pSqlBuffer.append("\"").append(wsColName).append("\"");
        }

        return IP_SUCCESS;
    }


    int     ip_format_valexp_list(long hquery, long hValExpList, StringBuffer pSqlBuffer)
    {
        long    hValExp;
        int     iFirst = TRUE;
        StringBuffer      wsAsColName = new StringBuffer(DAM_MAX_ID_LEN+1);

        hValExp = jdam.damex_getFirstValExp(hValExpList);
        while (hValExp != 0) 
        {
            if (iFirst == FALSE)
                pSqlBuffer.append(", ");
            else
                iFirst = FALSE;

            ip_format_valexp(hquery, hValExp, pSqlBuffer);

            jdam.damex_describeValExpEx(hValExp, wsAsColName, null);
            if (wsAsColName.length() > 0) 
                pSqlBuffer.append(" AS \"").append(wsAsColName).append("\" ");

            hValExp = jdam.damex_getNextValExp(hValExpList);
        }

        pSqlBuffer.append(" ");
        return IP_SUCCESS;
    }

    int     ip_format_valexp(long hquery, long hValExp, StringBuffer pSqlBuffer)
    {
        xo_int      piType = new xo_int(0); /* literal value, column, +, -, *, / etc   */
        xo_int      piFuncType = new xo_int(0);
        xo_long     hLeftValExp = new xo_long(0);
        xo_long     hRightValExp = new xo_long(0);
        xo_long     hVal = new xo_long(0);
        xo_long     hScalarValExp = new xo_long(0);
        xo_long     hCaseValExp = new xo_long(0);
        int         iFuncType;
        xo_int      piSign = new xo_int(0);

        jdam.damex_describeValExp(hValExp, piType, /* literal value, column, +, -, *, / etc   */
            piFuncType,
            hLeftValExp,
            hRightValExp,
            hVal,
            hScalarValExp,
            hCaseValExp
            );

        iFuncType = piFuncType.getVal();

        jdam.damex_describeValExpEx(hValExp, null, piSign);

        if(piSign.getVal() != 0)
        {
            pSqlBuffer.append("-(");
        }

        /* function type */
        if ((iFuncType & SQL_F_COUNT_ALL) != 0) pSqlBuffer.append("COUNT(*) ");
        if ((iFuncType & SQL_F_COUNT) != 0) pSqlBuffer.append("COUNT ");
        if ((iFuncType & SQL_F_AVG) != 0) pSqlBuffer.append("AVG ");
        if ((iFuncType & SQL_F_MAX) != 0) pSqlBuffer.append("MAX ");
        if ((iFuncType & SQL_F_MIN) != 0) pSqlBuffer.append("MIN ");
        if ((iFuncType & SQL_F_SUM) != 0) pSqlBuffer.append("SUM ");
        if ((iFuncType & SQL_F_VAR) != 0) pSqlBuffer.append("VAR_SAMP ");
        if ((iFuncType & SQL_F_VARP) != 0) pSqlBuffer.append("VAR_POP ");
        if ((iFuncType & SQL_F_STDDEV) != 0) pSqlBuffer.append("STDDEV_SAMP ");
        if ((iFuncType & SQL_F_STDDEVP) != 0) pSqlBuffer.append("STDDEV_POP ");

        if ((iFuncType != 0) && (iFuncType != SQL_F_COUNT_ALL))
            pSqlBuffer.append("( ");
        if ((iFuncType & SQL_F_DISTINCT) != 0) pSqlBuffer.append("DISTINCT ");
		
		if (iFuncType != SQL_F_COUNT_ALL) {
            switch (piType.getVal()) 
            {
               case SQL_VAL_EXP_VAL:
                    ip_format_val(hquery, hVal.getVal(), pSqlBuffer);
                    break;
				 
               case SQL_VAL_EXP_ADD:
                    pSqlBuffer.append("( ");
                    ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" + ");
                    ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" )");
                    break;
				   
               case SQL_VAL_EXP_SUBTRACT:
                    pSqlBuffer.append("( ");
                    ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" - ");
                    ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" )");
                    break;
				   

               case SQL_VAL_EXP_MULTIPLY:
                    pSqlBuffer.append("( ");
                    ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" * ");
                    ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" )");
                    break;

               case SQL_VAL_EXP_DIVIDE:
                    pSqlBuffer.append("( ");
                    ip_format_valexp(hquery, hLeftValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" / ");
                    ip_format_valexp(hquery, hRightValExp.getVal(), pSqlBuffer);
                    pSqlBuffer.append(" )");
                    break;

               case SQL_VAL_EXP_SCALAR:
                    ip_format_scalar_valexp(hquery, hScalarValExp.getVal(), pSqlBuffer);
                    break;

               case SQL_VAL_EXP_CASE:
                    ip_format_case_valexp(hquery, hCaseValExp.getVal(), pSqlBuffer);
                    break;

               default:            
			        pSqlBuffer.append("Invalid Value Expression Type:").append(piType.getVal());
                    break;
            }
		}

        if ((iFuncType != 0) && (iFuncType != SQL_F_COUNT_ALL))
            pSqlBuffer.append(")");


        if (piSign.getVal() != 0) 
        {
            pSqlBuffer.append(")");
        }
        return IP_SUCCESS;
    }

    int     ip_format_scalar_cast(long hquery, long hValExpList, StringBuffer pSqlBuffer)
    {
        StringBuffer    sName;
        long            hValExp;
        xo_int         iResXoType = new xo_int(0);
        xo_int         iLength = new xo_int(0);
        xo_int         iPrecision = new xo_int(0);
        xo_int         iScale = new xo_int(0);
        String         sTypeName;

        sName = new StringBuffer(ip.DAM_MAX_ID_LEN + 1);
        hValExp = jdam.damex_getFirstValExp(hValExpList);
        ip_format_valexp(hquery, hValExp, pSqlBuffer);

        jdam.dam_describeScalarEx(hValExpList, sName, null, iResXoType, iLength, iPrecision, iScale);
        sTypeName = xxx_map_type_to_name(iResXoType.getVal());
        pSqlBuffer.append(" AS " + sTypeName);
        if (iResXoType.getVal() == ip.XO_TYPE_NUMERIC || iResXoType.getVal() == ip.XO_TYPE_DECIMAL) 
        {
            pSqlBuffer.append("(" + iPrecision.getVal() + "," + iScale.getVal() + ")");
        }

        return IP_SUCCESS;
    }

    int     ip_format_scalar_valexp(long hquery, long   hScalarValExp, StringBuffer pSqlBuffer)
    {
        StringBuffer    sName;
        xo_long      phValExpList;

        phValExpList = new xo_long();
        sName = new StringBuffer(ip.DAM_MAX_ID_LEN + 1);

        jdam.damex_describeScalarValExp(hScalarValExp, sName, phValExpList);

        /* check if scalar function refers to a special @@ identifier */
        if((sName.substring(0,2)) .equals ("@@")) 
        {
            pSqlBuffer.append(sName);
            return IP_SUCCESS;
        }

        /* handle CONVERT function */
        if (sName.toString().equals("CONVERT")) 
        {
            pSqlBuffer.append("{fn ");
            pSqlBuffer.append(sName);
            pSqlBuffer.append("( ");
            //if (phValExpList.getVal() != 0) ip_format_scalar_convert(hquery, phValExpList.getVal(), pSqlBuffer);
            pSqlBuffer.append(") }" );
        }
        else if (sName.toString().equals("CAST")) 
        {
            pSqlBuffer.append(sName);
            pSqlBuffer.append("( ");
            if (phValExpList.getVal() != 0) ip_format_scalar_cast(hquery, phValExpList.getVal(), pSqlBuffer);
            pSqlBuffer.append(")" );
        }
        else 
        {
            pSqlBuffer.append(sName);
            pSqlBuffer.append("( ");
            if (phValExpList.getVal() != 0)
                ip_format_valexp_list(hquery, phValExpList.getVal(), pSqlBuffer);
            pSqlBuffer.append(") ");
        }
        return IP_SUCCESS;
    }

    int     ip_format_case_valexp(long hquery, long   hCaseValExp, StringBuffer pSqlBuffer)
    {
        xo_long             hInputValExp, hCaseElemList, hElseValExp;

        hInputValExp = new xo_long(0);
        hCaseElemList = new xo_long(0);
        hElseValExp = new xo_long(0);
        jdam.damex_describeCaseValExp(hCaseValExp, hInputValExp, hCaseElemList, hElseValExp);
        pSqlBuffer.append("CASE ");
        if (hInputValExp.getVal() != 0)
            ip_format_valexp(hquery, hInputValExp.getVal(), pSqlBuffer);
        pSqlBuffer.append(" ");
        ip_format_case_elem_list(hquery, hCaseElemList.getVal(), pSqlBuffer);
        if (hElseValExp.getVal() != 0) 
        {
            pSqlBuffer.append(" ELSE ");
            ip_format_valexp(hquery, hElseValExp.getVal(), pSqlBuffer);
        }
        pSqlBuffer.append(" END ");

        return IP_SUCCESS;
    }

    int     ip_format_case_elem_list(long hquery, long   hCaseElemList, StringBuffer pSqlBuffer)
    {
        long             hCaseElem;
        xo_long          hWhenValExp;
        xo_long          hWhenBoolExp;
        xo_long          hResValExp;

        hWhenValExp = new xo_long(0);
        hWhenBoolExp = new xo_long(0);
        hResValExp = new xo_long(0);
        hCaseElem = jdam.damex_getFirstCaseElem(hCaseElemList);
        while (hCaseElem != 0) 
        {
            pSqlBuffer.append(" WHEN ");

            jdam.damex_describeCaseElem(hCaseElem, hWhenValExp, hWhenBoolExp, hResValExp);
            if (hWhenValExp.getVal() != 0) ip_format_valexp(hquery, hWhenValExp.getVal(), pSqlBuffer);
            if (hWhenBoolExp.getVal() != 0) ip_format_logexp(hquery, hWhenBoolExp.getVal(), pSqlBuffer);
            pSqlBuffer.append(" THEN ");
            ip_format_valexp(hquery, hResValExp.getVal(), pSqlBuffer);

            hCaseElem = jdam.damex_getNextCaseElem(hCaseElemList);
        }

        return IP_SUCCESS;
    }

    int     ip_format_val(long   hquery, long hVal, StringBuffer pSqlBuffer)
    {
        int         iType; /* literal value, column */
        int         iXoType; /* type of literal value - INTEGER, CHAR etc */
        xo_int      piType, piXoType, piValLen;
        xo_long     hCol;
        xo_long     hSubQuery;
        xo_int      piValStatus;
        Object      pData;

        piType = new xo_int();
        piXoType = new xo_int();
        piValLen =  new xo_int(0);
        hCol = new xo_long();
        hSubQuery = new xo_long();
        piValStatus = new xo_int();


        pData = jdam.damex_describeVal(hVal, piType,
            piXoType,
            piValLen,
            hCol,
            hSubQuery, piValStatus);

        iType = piType.getVal();
        iXoType = piXoType.getVal();

        switch (iType) 
        {

            case SQL_VAL_DATA_CHAIN:
                pSqlBuffer.append("?");
                /*                ghValBlob = hVal; */
                break;
            case SQL_VAL_NULL:
                pSqlBuffer.append("NULL"); break;
            case SQL_VAL_QUERY: /* query */
                pSqlBuffer.append("( ");
                ip_format_query(hSubQuery.getVal(),pSqlBuffer);
                pSqlBuffer.append(" )");
                break;
            case SQL_VAL_COL: /* value is the column value */
                ip_format_col(hquery, hCol.getVal(), pSqlBuffer); break;
            case SQL_VAL_INTERVAL:
                break;
            case SQL_VAL_LITERAL: /* value is a Xo Type literal */
                {
                String  strObject;
                Integer iObject;
                xo_tm   xoTime;
                Double  dObject;
                Float   fObject;
                Short   sObject;
                Boolean bObject;
                Byte    byObject;
                Long    lObject;

                switch (iXoType) 
                {
                    case XO_TYPE_CHAR: /* pVal is a char literal */
                    case XO_TYPE_VARCHAR:
                    case XO_TYPE_NUMERIC:
                    case XO_TYPE_DECIMAL:
                    case XO_TYPE_LONGVARCHAR:

                        strObject = (String) pData;
                        ip_format_string_literal(strObject,pSqlBuffer);
                        break;
                    case XO_TYPE_WCHAR: /* pVal is a wchar literal */
                    case XO_TYPE_WVARCHAR:
                    case XO_TYPE_WLONGVARCHAR:
                        strObject = (String) pData;
                        pSqlBuffer.append("N'").append(strObject).append("'");
                        break;
                    case XO_TYPE_INTEGER:  /* pVal is a integer literal */
                        iObject = (Integer) pData;
                        pSqlBuffer.append(iObject.intValue());
                        break;
                    case XO_TYPE_SMALLINT: /* pVal is small integer literal */
                        sObject = (Short) pData;
                        pSqlBuffer.append(sObject.shortValue());
                        break;
                    case XO_TYPE_FLOAT: /* pVal is a double literal */
                    case XO_TYPE_DOUBLE:
                        dObject = (Double) pData;
                        pSqlBuffer.append(dObject.doubleValue());
                        break;
                    case XO_TYPE_REAL: /* pVal is a float literal */
                        fObject = (Float) pData;
                        pSqlBuffer.append(fObject.floatValue());
                        break;
                    case XO_TYPE_DATE:
                        xoTime = (xo_tm)pData;
                        pSqlBuffer.append("{d '").append(xoTime.getVal(xo_tm.YEAR)).append("-").append(xoTime.getVal(xo_tm.MONTH)+1).append("-").append(xoTime.getVal(xo_tm.DAY_OF_MONTH)).append("'}");
                        break;
                    case XO_TYPE_TIME:
                        xoTime = (xo_tm)pData;
                        pSqlBuffer.append("{t '").append(" ").append(xoTime.getVal(xo_tm.HOUR)).append(":").append(xoTime.getVal(xo_tm.MINUTE)).append(":").append(xoTime.getVal(xo_tm.SECOND)).append("'}");
                        break;
                    case XO_TYPE_TIMESTAMP:
                        xoTime = (xo_tm)pData;
                        if (xoTime.getVal(xo_tm.FRACTION) > 0) 
                        {
                            int     frac;

                            frac = (int) (xoTime.FRACTION * 0.000001);
                            pSqlBuffer.append("{ts '").append(xoTime.getVal(xo_tm.YEAR)).append("-").append(xoTime.getVal(xo_tm.MONTH)+1).append("-").append(xoTime.getVal(xo_tm.DAY_OF_MONTH))
                                .append(" ").append(xoTime.getVal(xo_tm.HOUR)).append(":").append(xoTime.getVal(xo_tm.MINUTE)).append(":").append(xoTime.getVal(xo_tm.SECOND))
                                .append(".").append(xoTime.getVal(xo_tm.FRACTION)).append("'}");
                        }
                        else 
                        {
                            pSqlBuffer.append("{ts '").append(xoTime.getVal(xo_tm.YEAR)).append("-").append(xoTime.getVal(xo_tm.MONTH)+1).append("-").append(xoTime.getVal(xo_tm.DAY_OF_MONTH))
                                .append(" ").append(xoTime.getVal(xo_tm.HOUR)).append(":").append(xoTime.getVal(xo_tm.MINUTE)).append(":").append(xoTime.getVal(xo_tm.SECOND)).append("'}");
                        }

                        break;

                    case XO_TYPE_BIT:
                        bObject = (Boolean)pData;
                        pSqlBuffer.append(bObject.booleanValue()?1:0);
                        break;

                    case XO_TYPE_TINYINT:

                        byObject = (Byte)pData;
                        pSqlBuffer.append(byObject.byteValue());
                        break;

                    case XO_TYPE_BIGINT:
                        lObject = (Long)pData;
                        pSqlBuffer.append(lObject.longValue());
                        break;

                    default:
                        pSqlBuffer.append("Invalid Xo Value Type:").append(iXoType);
                        break;
                }
            }
                break;
            default:
                pSqlBuffer.append("Invalid Value Type:").append(iType); break;
        }
        return IP_SUCCESS;

    }

    int     ip_format_table_list(long hquery, StringBuffer pSqlBuffer)
    {
        long              htable;
        int               iFirst = TRUE;
        xo_int            piTableNum = new xo_int(0);
        xo_int piNestedTableNum = new xo_int(0);
        StringBuffer      wsCatalog   = new StringBuffer(DAM_MAX_ID_LEN+1);
        StringBuffer      wsSchema    = new StringBuffer(DAM_MAX_ID_LEN+1);
        StringBuffer      wsTableName = new StringBuffer(DAM_MAX_ID_LEN+1);
        int               iJoinType;
        xo_int            piJoinType = new xo_int(0);
        xo_long           phJoinExp = new xo_long(0);
        long        hTableSubQuery;
        xo_long phTableList = new xo_long(0);

        htable = jdam.damex_getFirstTable(hquery);
        while (htable != 0) 
        {
            jdam.damex_describeTableEx(htable, piTableNum, piNestedTableNum,
                    wsCatalog, wsSchema, wsTableName, null, null);

            /* check if table subquery */ 
            hTableSubQuery = jdam.damex_isTableSubQuery(htable); 

            if(hTableSubQuery != 0)
            {
                if (iFirst == FALSE) 
                {
                    pSqlBuffer.append(", ");
                }
                pSqlBuffer.append("( ");
                ip_format_query(hTableSubQuery,pSqlBuffer);
                pSqlBuffer.append(" ) ");
                pSqlBuffer.append("T").append(piNestedTableNum.getVal()).append("_Q").append(hquery);
                iFirst = FALSE;
                htable = jdam.damex_getNextTable(hquery);
                continue;
            }
            phJoinExp.setVal(0);
            
            jdam.damex_describeTableJoinInfo(htable, piJoinType, phJoinExp);

            iJoinType = piJoinType.getVal();

            switch (iJoinType) 
            {
                case SQL_JOIN_LEFT_OUTER:
                    pSqlBuffer.append(" LEFT OUTER JOIN ");
                    break;
                case SQL_JOIN_RIGHT_OUTER:
                    pSqlBuffer.append(" RIGHT OUTER JOIN ");
                    break;
                case SQL_JOIN_FULL_OUTER:
                    pSqlBuffer.append(" FULL OUTER JOIN ");
                    break;
                case SQL_JOIN_INNER:
                    pSqlBuffer.append(" INNER JOIN ");
                    break;
                case SQL_JOIN_OLD_STYLE:
                    if (iFirst == FALSE)
                        pSqlBuffer.append(", ");
                    break;
            }
            if(wsCatalog != null && wsCatalog.length() > 0)
                pSqlBuffer.append(wsCatalog).append(".");
            if(wsSchema != null && wsSchema.length() > 0)
                pSqlBuffer.append(wsSchema).append(".");

            phTableList.setVal(jdam.damex_isNestedJoinTable(htable));
            if (phTableList.getVal() != 0) {
                pSqlBuffer.append("( ");
                ip_format_nested_join_table_list(hquery, phTableList, pSqlBuffer);
                pSqlBuffer.append(") ");
            } else {
                pSqlBuffer.append(wsTableName).append(" T").append(piNestedTableNum.getVal()).append("_Q").append(hquery);
            }

            if (phJoinExp.getVal() != 0) {
                pSqlBuffer.append(" ON ");
                ip_format_logexp(hquery, phJoinExp.getVal(), pSqlBuffer);
            }

            iFirst = FALSE;
            htable = jdam.damex_getNextTable(hquery);
        }

        pSqlBuffer.append(" ");
        return IP_SUCCESS;
    }

    /********************************************************************************************
     * Method: ip_format_nested_join_table_list 
     * Description: Nested join table
     * list format Return: IP_SUCCESS on success 
     *                        IP_FAILURE on error
     *********************************************************************************************/
    int ip_format_nested_join_table_list(long hquery, xo_long phTableList,
        StringBuffer pSqlBuffer) {
        long htable;
        int iFirst = TRUE;
        xo_int piTableNum = new xo_int(0);
        xo_int piNestedTableNum = new xo_int(0);
        StringBuffer wsTableName = new StringBuffer(DAM_MAX_ID_LEN + 1);
        xo_int piJoinType = new xo_int(0);
        xo_long phJoinExp = new xo_long(0);
        long hTableSubQuery;
        xo_long phNewTableList = new xo_long(0);

        htable = jdam.damex_getFirstTableEx(phTableList.getVal());
        while (htable != 0) {
            jdam.damex_describeTableEx(htable, piTableNum, piNestedTableNum,
                    null, null, wsTableName, null, null);

            phJoinExp.setVal(0);
            jdam.damex_describeTableJoinInfo(htable, piJoinType, phJoinExp);

            if (phJoinExp.getVal() != 0) {
                switch (piJoinType.getVal()) {
                case SQL_JOIN_LEFT_OUTER:
                    pSqlBuffer.append(" LEFT OUTER JOIN ");
                    break;
                case SQL_JOIN_RIGHT_OUTER:
                    pSqlBuffer.append(" RIGHT OUTER JOIN ");
                    break;
                case SQL_JOIN_FULL_OUTER:
                    pSqlBuffer.append(" FULL OUTER JOIN ");
                    break;
                case SQL_JOIN_INNER:
                    pSqlBuffer.append(" INNER JOIN ");
                    break;
                case SQL_JOIN_OLD_STYLE:
                    if (iFirst == FALSE)
                        pSqlBuffer.append(", ");
                    break;
                }
            }

            /* check if table sub query */
            hTableSubQuery = jdam.damex_isTableSubQuery(htable);
            if (hTableSubQuery != 0) {
                pSqlBuffer.append("( ");
                ip_format_query(hTableSubQuery, pSqlBuffer);
                pSqlBuffer.append(" ) ");
                pSqlBuffer.append(" T").append(piNestedTableNum.getVal())
                		.append("_Q").append(hquery);
            } else {
                phNewTableList.setVal(jdam.damex_isNestedJoinTable(htable));
                if (phNewTableList.getVal() != 0) {
                    pSqlBuffer.append("( ");
                    ip_format_nested_join_table_list(hquery, phNewTableList, pSqlBuffer);
                    pSqlBuffer.append(") ");
                } else {
                    pSqlBuffer.append(wsTableName).append(" T").append(piNestedTableNum.getVal()).append("_Q").append(hquery);
                }
            }

            if (phJoinExp.getVal() != 0) {
                pSqlBuffer.append(" ON ");
                ip_format_logexp(hquery, phJoinExp.getVal(), pSqlBuffer);
            }

            iFirst = FALSE;
            htable = jdam.damex_getNextTableEx(phTableList.getVal());
        }

        pSqlBuffer.append(" ");
        return DAM_SUCCESS;
    }

	/********************************************************************************************
	 * Method: ip_format_logexp Description: Logical expression handling Return:
	 * IP_SUCCESS on success IP_FAILURE on error
	 *********************************************************************************************/
    int     ip_format_logexp(long hquery, long hLogExp, StringBuffer pSqlBuffer)
    {
        xo_int         iType; /* AND, OR , NOT or CONDITION */
        xo_long        hLeft, hRight;
        xo_long        hCond;

        iType = new xo_int(0);
        hLeft = new xo_long(0);
        hRight = new xo_long(0);
        hCond = new xo_long(0);

        jdam.damex_describeLogicExp(hLogExp,
            iType, /* AND, OR , NOT or CONDITION */
            hLeft,
            hRight,
            hCond);

        switch (iType.getVal()) 
        {
            case SQL_EXP_COND:
                pSqlBuffer.append("( ");
                ip_format_cond(hquery, hCond.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");
                break;
            case SQL_EXP_AND:
                pSqlBuffer.append("( ");
                ip_format_logexp(hquery, hLeft.getVal(), pSqlBuffer);
                pSqlBuffer.append(" AND ");
                ip_format_logexp(hquery, hRight.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;
            case SQL_EXP_OR:
                pSqlBuffer.append("( ");
                ip_format_logexp(hquery, hLeft.getVal(), pSqlBuffer);
                pSqlBuffer.append(" OR ");
                ip_format_logexp(hquery, hRight.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");

                break;
            case SQL_EXP_NOT:
                pSqlBuffer.append("( ");
                pSqlBuffer.append(" NOT ");
                ip_format_logexp(hquery, hLeft.getVal(), pSqlBuffer);
                pSqlBuffer.append(" )");
                break;
            default:            pSqlBuffer.append("Invalid Expression Type:").append(iType);
                break;

        }

        return IP_SUCCESS;
    }

/********************************************************************************************
    Method:         ip_format_cond
    Description:    Condition, Operator handling
    Return:         IP_SUCCESS on success
                    IP_FAILURE on error
*********************************************************************************************/
    int ip_format_cond(long hquery, long hCond, StringBuffer pSqlBuffer)
    {
        xo_int     piType;
        xo_long    hLeft, hRight, hExtra;
        int        iType;

        piType = new xo_int(0);
        hLeft = new xo_long(0);
        hRight = new xo_long(0);
        hExtra = new xo_long(0);

        jdam.damex_describeCond(hCond,
            piType, /* >, <, =, BETWEEN etc.*/
            hLeft,
            hRight,
            hExtra); /* used for BETWEEN */

        iType = piType.getVal();

        /* EXISTS and UNIQUE predicates */
        if ((iType & (SQL_OP_EXISTS | SQL_OP_UNIQUE)) != 0) 
        {

            if ((iType & SQL_OP_NOT) != 0) pSqlBuffer.append(" NOT ");
            if ((iType & SQL_OP_EXISTS) != 0) pSqlBuffer.append(" EXISTS (");
            if ((iType & SQL_OP_UNIQUE) != 0) pSqlBuffer.append(" UNIQUE (");

            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            pSqlBuffer.append(" )");
        }


        /* conditional predicates */
        if ((iType & ( SQL_OP_SMALLER | SQL_OP_GREATER |  SQL_OP_EQUAL)) != 0) 
        {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);

            if ((iType & SQL_OP_NOT) != 0) 
            {
                if ((iType & SQL_OP_EQUAL) != 0)
                    pSqlBuffer.append(" <> ");
            }
            else 
            {
                pSqlBuffer.append(" ");
                if ((iType & SQL_OP_SMALLER) != 0) pSqlBuffer.append("<");
                if ((iType & SQL_OP_GREATER) != 0) pSqlBuffer.append(">");
                if ((iType & SQL_OP_EQUAL) != 0) pSqlBuffer.append("=");
                pSqlBuffer.append(" ");
            }

            if ((iType & (SQL_OP_QUANTIFIER_ALL | SQL_OP_QUANTIFIER_SOME | SQL_OP_QUANTIFIER_ANY)) != 0) 
            {
                if ((iType & SQL_OP_QUANTIFIER_ALL) != 0)
                    pSqlBuffer.append(" ALL ( ");
                if ((iType & SQL_OP_QUANTIFIER_SOME) != 0)
                    pSqlBuffer.append(" SOME ( ");
                if ((iType & SQL_OP_QUANTIFIER_ANY) != 0)
                    pSqlBuffer.append(" ANY ( ");
            }

            ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);
        }
        /* like predicate */
        if ((iType & SQL_OP_LIKE) != 0) 
        {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            if ((iType & SQL_OP_NOT) != 0)
                pSqlBuffer.append(" NOT ");
            pSqlBuffer.append(" LIKE ");
            ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);

            if (hExtra.getVal() != 0) 
            {
                pSqlBuffer.append(" ESCAPE ");
                ip_format_valexp(hquery, hExtra.getVal(), pSqlBuffer);
            }

        }

        /* Is NULL predicate */
        if ((iType & SQL_OP_ISNULL) != 0) 
        {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            if ((iType & SQL_OP_NOT) != 0)
                pSqlBuffer.append(" IS NOT NULL ");
            else
                pSqlBuffer.append(" IS NULL ");
        }

        /* IN predicate */
        if ((iType & SQL_OP_IN) != 0) 
        {
            ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
            if ((iType & SQL_OP_NOT) != 0)
                pSqlBuffer.append(" NOT ");
            pSqlBuffer.append(" IN ");
            ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);
        }

        /* BETWEEN predicate */
        if ((iType & SQL_OP_BETWEEN) != 0) 
        {

            /* check if the between is a form of ( >= and < ) OR (> and <)
             OR (> and <=)
             */
            if (((iType & SQL_OP_BETWEEN_OPEN_LEFT) != 0) || ((iType & SQL_OP_BETWEEN_OPEN_RIGHT) != 0)) 
            {
                /* format it as two conditions */
                ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
                if ((iType & SQL_OP_BETWEEN_OPEN_LEFT) != 0)
                    pSqlBuffer.append(" > ");
                else
                    pSqlBuffer.append(" >= ");
                ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);

                pSqlBuffer.append(" AND ");

                ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);
                if ((iType & SQL_OP_BETWEEN_OPEN_RIGHT) != 0)
                    pSqlBuffer.append(" < ");
                else
                    pSqlBuffer.append(" <= ");
                ip_format_valexp(hquery, hExtra.getVal(), pSqlBuffer);
            }
            else 
            {
                /* standard BETWEEN pattern */
                ip_format_valexp(hquery, hLeft.getVal(), pSqlBuffer);

                if ((iType & SQL_OP_NOT) != 0)
                    pSqlBuffer.append(" NOT ");
                pSqlBuffer.append(" BETWEEN ");

                ip_format_valexp(hquery, hRight.getVal(), pSqlBuffer);
                pSqlBuffer.append(" AND ");
                ip_format_valexp(hquery, hExtra.getVal(), pSqlBuffer);
            }

        }

        return IP_SUCCESS;
    }

        int     ip_format_group_list(long hquery, long hValExpList, StringBuffer pSqlBuffer)
        {
            ip_format_valexp_list(hquery, hValExpList, pSqlBuffer);
            return IP_SUCCESS;
        }

        int     ip_format_order_list(long   hquery, long hValExpList, StringBuffer pSqlBuffer)
        {
            long            hValExp;
            xo_int          piResultColNum = new xo_int(0);
            xo_int          piSortOrder = new xo_int(0);
            int             iFirst = TRUE;

            hValExp = jdam.damex_getFirstValExp(hValExpList);
            while (hValExp != 0) 
            {

                if (iFirst == FALSE)
                    pSqlBuffer.append(", ");
                else
                    iFirst = FALSE;

                jdam.damex_describeOrderByExp(hValExp, piResultColNum, piSortOrder);

                if (piResultColNum.getVal() != DAM_NOT_SET) /* use the result column number */
                    pSqlBuffer.append(piResultColNum.getVal()+1);
                else
                    ip_format_valexp(hquery, hValExp, pSqlBuffer);

                if (piSortOrder.getVal() == SQL_ORDER_ASC)
                    pSqlBuffer.append(" ASC");
                else if (piSortOrder.getVal() == SQL_ORDER_DESC)
                    pSqlBuffer.append(" DESC");

                hValExp = jdam.damex_getNextValExp(hValExpList);
            }

            pSqlBuffer.append(" ");
            return IP_SUCCESS;
        }

        int ip_format_string_literal(String pString,StringBuffer pSqlBuffer)
        {
            pSqlBuffer.append("'");
            String resultStr = pString.replaceAll("'","''");
            pSqlBuffer.append(resultStr);
            pSqlBuffer.append("'");
            return IP_SUCCESS;
        }

        int     ip_format_table_query(long hquery, StringBuffer pSqlBuffer)
        {
            long         hVal; 
            int          iFirst = TRUE; 

            hVal = jdam.damex_getFirstTableQueryVal(hquery); 
            while (hVal != 0) 
            { 
                if (iFirst == FALSE) 
                    pSqlBuffer.append(", "); 
                else 
                    iFirst = FALSE; 
        
                ip_format_val(hquery, hVal, pSqlBuffer); 
                hVal = jdam.damex_getNextTableQueryVal(hquery); 
            }
            return IP_SUCCESS;
        }
        /************************************************************************
        Function:       ip_isOriginalSelectListCompatible()
        Description:
        Return:
        ************************************************************************/
        boolean             ip_isOriginalSelectListCompatible(long hSelectValExpList,
                                                long hGroupValExpList,
                                                long hOrderValExpList)
        {

            /* Check if there are Set functions in the Query */
            {
                long    hValExp;
                xo_int  piType = new xo_int(0);
                xo_int  piFuncType = new xo_int(0);
                xo_long phLeftValExp = new xo_long(0);
                xo_long phRightValExp = new xo_long(0);
                xo_long phVal = new xo_long(0);
                xo_long phScalarValExp = new xo_long(0);
                xo_long phCaseValExp = new xo_long(0);

                hValExp = jdam.damex_getFirstValExp(hSelectValExpList);
                while (hValExp != 0) 
                {
                    jdam.damex_describeValExp(hValExp, piType, piFuncType, phLeftValExp, phRightValExp,phVal,phScalarValExp, phCaseValExp);

                    if (piFuncType.getVal() != 0) return false;
                    hValExp = jdam.damex_getNextValExp(hSelectValExpList);
                }
            }

            /* Check for GROUP BY */
            if (hGroupValExpList != 0) return false;

            /* check if ORDER BY does not refer to result columns */
            if (hOrderValExpList != 0) 
            {
                long        hValExp;
                xo_int      piResultColNum = new xo_int(0);
                xo_int      piSortOrder = new xo_int(0);

                hValExp = jdam.damex_getFirstValExp(hOrderValExpList);
                while (hValExp != 0) 
                {

                    jdam.damex_describeOrderByExp(hValExp, piResultColNum, piSortOrder);
                    if (piResultColNum.getVal() == DAM_NOT_SET) return false;

                    hValExp = jdam.damex_getNextValExp(hOrderValExpList);
                }
            }

            return true;
        }

        int     ip_process_buffer_rows(XXX_STMT_DA pStmtDA,boolean bFirst)
        {
            long   hrow = 0;
            int    iResCol;
            long   hcol;
            int    iRetCode = 0;
            boolean bIsColumnValInStrings = false;
        
            if(!m_bUseOriginalSelectList)
            {
                long        htable;
                int         iColNum = 0;
                int	    iRows = 0;
                
                while(true)
                {
                    try
                    {
                        if(bFirst)
                        {
                            if (!pStmtDA.rs.next()) 
                                return DAM_SUCCESS;
                            pStmtDA.m_ResultBuffer.setResColumnType(false);
                        }

                        iColNum = 0;
                        htable = jdam.damex_getFirstTable(jdam.damex_getQuery(pStmtDA.dam_hstmt));
                        while (htable != 0) 
                        {
                            hcol = jdam.damex_getFirstCol(htable, DAM_COL_IN_USE);
	
                            while (hcol != 0) 
                            {
                                try
                                {
                                    ip_process_buffer_column_val(pStmtDA,iColNum);
                                }
                                catch(BufferOverflowException e)
                                {
                                    pStmtDA.m_ResultBuffer.setNoRowsInBuffer(iRows);
                                    pStmtDA.iRowCount=iRows;
                                    jdam.damex_addResultBufferToTable(pStmtDA.dam_hstmt,pStmtDA.m_ResultBuffer);
                                    iRetCode = DAM_SUCCESS_WITH_RESULT_PENDING;
                                    pStmtDA.m_ResultBuffer.clear();
                                    return iRetCode;
                                }
                                hcol = jdam.damex_getNextCol(htable);
                                iColNum++;
                            }
                            htable = jdam.damex_getNextTable(jdam.damex_getQuery(pStmtDA.dam_hstmt));
                        }
                        if(bFirst)
                        {                         
                            pStmtDA.m_ResultBuffer.setNoOfResColumns(iColNum);
                        }
                        iRows++;
                        bFirst = false;
                        if (!pStmtDA.rs.next())
                        {
                            pStmtDA.m_ResultBuffer.setNoRowsInBuffer(iRows);
                            pStmtDA.iRowCount=iRows;
                            jdam.damex_addResultBufferToTable(pStmtDA.dam_hstmt,pStmtDA.m_ResultBuffer);
                            iRetCode = DAM_SUCCESS;
                            pStmtDA.m_ResultBuffer.clear();
                            return iRetCode;
                        }
                    }
                    catch(SQLException e)
                    {
                        return DAM_ERROR;
                    }
                }
            }
            else 
            {
                int	    iRows = 0;
                int[] 	    colXOTypes = null;

                while(true)
                {
                    try
                    {
                        if(bFirst)
                        {
                            if (!pStmtDA.rs.next()) 
                                return DAM_SUCCESS;
                            pStmtDA.m_ResultBuffer.setResColumnType(true);
                            pStmtDA.m_ResultBuffer.setNoOfResColumns(pStmtDA.iColCount);
                            colXOTypes = new int[pStmtDA.iColCount];
                            for (iResCol = 0; iResCol < pStmtDA.iColCount; iResCol++) 
                            {
                                colXOTypes[iResCol] = MapJdbcTypeToXoType(m_JdbcUtil.getColumnType(pStmtDA.rs,iResCol+1));
                            }
                            pStmtDA.m_ResultBuffer.setColumnXoTypes(colXOTypes);
                        }
                        try
                        {
                            for (iResCol = 0; iResCol < pStmtDA.iColCount; iResCol++) 
                            {
                                ip_process_buffer_column_val(pStmtDA,iResCol);
                            }
                        }
                        catch(BufferOverflowException e)
                        {
                            pStmtDA.m_ResultBuffer.setNoRowsInBuffer(iRows);
                            pStmtDA.iRowCount=iRows;
                            jdam.damex_addResultBufferToTable(pStmtDA.dam_hstmt,pStmtDA.m_ResultBuffer);
                            iRetCode = DAM_SUCCESS_WITH_RESULT_PENDING;
                            pStmtDA.m_ResultBuffer.clear();
                            return iRetCode;
                        }
                        iRows++;
                        bFirst = false;
                        if (!pStmtDA.rs.next())
                        {
                            pStmtDA.m_ResultBuffer.setNoRowsInBuffer(iRows);
                            pStmtDA.iRowCount=iRows;
                            jdam.damex_addResultBufferToTable(pStmtDA.dam_hstmt,pStmtDA.m_ResultBuffer);
                            iRetCode = DAM_SUCCESS;
                            pStmtDA.m_ResultBuffer.clear();
                            return iRetCode;
                        }
                    }
                    catch(SQLException e)
                    {
                        return DAM_ERROR;
                    }
                }
            }
        }
/************************************************************************
Function:       xxx_process_row()
Description:    Build the row
Return:         DAM_SUCCESS on Success
                DAM_FAILURE   on Failure
************************************************************************/
    int     ip_process_row(XXX_STMT_DA pStmtDA)
    {
        long   hrow = 0;
        int    iResCol;
        long   hcol;
        int    iRetCode = 0;
        boolean bIsColumnValInStrings = false;

        /* allocate a new row */
        hrow = jdam.damex_allocRow(pStmtDA.dam_hstmt);

        if(!m_bUseOriginalSelectList) 
        {
            long        htable;
            int         iColNum = 0;

            htable = jdam.damex_getFirstTable(jdam.damex_getQuery(pStmtDA.dam_hstmt));
            while (htable != 0) 
            {
                xo_int         piTableNum = new xo_int(0);
                StringBuffer  wsTableName = new StringBuffer(DAM_MAX_ID_LEN+1);

                jdam.damex_describeTable(htable, piTableNum, null, null, wsTableName, null, null);
                hcol = jdam.damex_getFirstCol(htable, DAM_COL_IN_USE);

                while (hcol != 0) 
                {

                    if(bIsColumnValInStrings)
                    {
                        String      pVal;
                        pVal = m_JdbcUtil.getColumnValue(pStmtDA.rs, iColNum+1);
                        if (pVal == null)
                            iRetCode = jdam.damex_addCharColValToRow(pStmtDA.dam_hstmt, hrow, hcol, null, XO_NULL_DATA );
                        else
                            iRetCode = jdam.damex_addCharColValToRow(pStmtDA.dam_hstmt, hrow, hcol, pVal, XO_NTS);
                    }
                    else
                    {
                        ip_process_column_val(pStmtDA,hrow,hcol,iColNum);
                    }

                    hcol = jdam.damex_getNextCol(htable);
                    iColNum++;
                }
                htable = jdam.damex_getNextTable(jdam.damex_getQuery(pStmtDA.dam_hstmt));
            }
        }
        else 
        {
            for (iResCol = 0; iResCol < pStmtDA.iColCount; iResCol++) 
            {
                if (bIsColumnValInStrings) 
                {
                    String pVal;

                    pVal = m_JdbcUtil.getColumnValue(pStmtDA.rs, iResCol + 1);
                    if (pVal == null)
                        iRetCode = jdam.damex_addCharResValToRow(
                            pStmtDA.dam_hstmt, hrow, iResCol, null,
                            XO_NULL_DATA);
                    else
                        iRetCode = jdam.damex_addCharResValToRow(
                            pStmtDA.dam_hstmt, hrow, iResCol, pVal, XO_NTS);
                } 
                else 
                {
                    ip_process_column_res_val(pStmtDA,hrow,iResCol);
                }

            }
        }


        iRetCode = jdam.damex_addRowToTable(pStmtDA.dam_hstmt, hrow);
        if (iRetCode != DAM_SUCCESS) return iRetCode;

        pStmtDA.iRowCount++;

        return DAM_SUCCESS;
    }

    public int ip_process_buffer_column_val(XXX_STMT_DA pStmtDA,int iColNum)
    {
        int    iRetCode = 0;
        Object colObject = m_JdbcUtil.getColumnObject(pStmtDA.rs,iColNum + 1);
        StringBuffer pData = new StringBuffer();
	
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "ip_process_buffer_column_val() Called.>\n");

        if(colObject instanceof java.lang.Integer)
        {
            pStmtDA.m_ResultBuffer.putInt(m_JdbcUtil.getIntColumnValue(pStmtDA.rs,iColNum+1));
        }
        else if(colObject instanceof java.math.BigInteger || colObject instanceof java.lang.Long )
        {
            pStmtDA.m_ResultBuffer.putBigInt((long)m_JdbcUtil.getBigDecimalColumnValue(pStmtDA.rs,iColNum+1).longValue());
        }
        else if(colObject instanceof java.lang.Short)
        {
            pStmtDA.m_ResultBuffer.putShort((short)m_JdbcUtil.getShortColumnValue(pStmtDA.rs,iColNum+1));
        }
        else if(colObject instanceof java.lang.String && (Types.LONGVARCHAR == m_JdbcUtil.getColumnType(pStmtDA.rs,iColNum+1)) )
        {
            pStmtDA.m_ResultBuffer.putLongString((String)colObject);
        }
        else if(colObject instanceof java.lang.String)
        {
            pStmtDA.m_ResultBuffer.putString(m_JdbcUtil.getColumnValue(pStmtDA.rs,iColNum+1));
        }
        else if(colObject instanceof java.math.BigDecimal)
        {
            pStmtDA.m_ResultBuffer.putString(new BigDecimal(m_JdbcUtil.getColumnValue(pStmtDA.rs,iColNum+1)).toPlainString());
        }
        else if(colObject instanceof java.lang.Float)
        {
            pStmtDA.m_ResultBuffer.putReal(m_JdbcUtil.getFloatColumnValue(pStmtDA.rs,iColNum+1));
        }
        else if(colObject instanceof java.lang.Double || colObject instanceof java.lang.Float)
        {
            pStmtDA.m_ResultBuffer.putDouble(m_JdbcUtil.getDoubleColumnValue(pStmtDA.rs,iColNum+1));
        }
        else if(colObject instanceof java.lang.Boolean)
        {
            pStmtDA.m_ResultBuffer.putShort(m_JdbcUtil.getBooleanColumnValue(pStmtDA.rs,iColNum+1) ? (short)1 : (short)0);
        }
        else if(colObject instanceof java.sql.Date)
        {
            Date val = m_JdbcUtil.getDateColumnValue(pStmtDA.rs, iColNum+1);
            xo_tm date = new xo_tm(val.getYear()+1900,val.getMonth(),val.getDate());
            pStmtDA.m_ResultBuffer.putDate(date);
        }
        else if(colObject instanceof java.sql.Time)
        {
            Time val = m_JdbcUtil.getTimeColumnValue(pStmtDA.rs, iColNum+1);
            xo_tm time = new xo_tm(val.getHours(),val.getMinutes(),val.getSeconds(),0);
            pStmtDA.m_ResultBuffer.putTime(time);
        }
        else if(colObject instanceof Timestamp)
        {
            Timestamp val = m_JdbcUtil.getTimeStampColumnValue(pStmtDA.rs, iColNum+1);
            xo_tm timestamp = new xo_tm(val.getYear()+1900,val.getMonth(),val.getDate(),val.getHours(),val.getMinutes(),val.getSeconds());
            pStmtDA.m_ResultBuffer.putTimeStamp(timestamp);
        }
        else if(colObject instanceof java.sql.Blob)
        {
            InputStream pSource = null;
            byte[]  pBuffer;
            long    nBufferSize;
            long    nAmtRead;

            try
            {            
                Blob val = m_JdbcUtil.getBlobColumnValue(pStmtDA.rs, iColNum+1);
                pSource = val.getBinaryStream();
                if (pSource == null) 
                {
                    /* adda null value */                
                    pStmtDA.m_ResultBuffer.putNull();
                }
                /* allocate a buffer */
                pBuffer = new byte[FILEIO_MAX_BUFFER_SIZE];
                nBufferSize = FILEIO_MAX_BUFFER_SIZE;

                nAmtRead = pSource.read(pBuffer, 0, (int)nBufferSize);
                if (nAmtRead > 0) 
                {
                    pStmtDA.m_ResultBuffer.putLongBinary(pBuffer);
                }
                /* Close the stream */
                pSource.close();
            }
            catch(Exception e)
            { 
                pStmtDA.m_ResultBuffer.putNull();
            }
        }
        else if(colObject instanceof java.sql.Clob)
        {
            if(passjdbc_clob_data(pStmtDA,iColNum,pData))
            {
                pStmtDA.m_ResultBuffer.putLongString(pData.toString());
                pData.delete(0,pData.length());
            }
            else
                pStmtDA.m_ResultBuffer.putNull();
        }
        else if(colObject instanceof byte[] && (Types.LONGVARBINARY == m_JdbcUtil.getColumnType(pStmtDA.rs,iColNum+1)) )
        {
            pStmtDA.m_ResultBuffer.putLongBinary((byte [])colObject);
        }
        else if(colObject instanceof byte[])
        {
            byte[] bytesVal = m_JdbcUtil.getBytesColumnValue(pStmtDA.rs, iColNum+1);
            pStmtDA.m_ResultBuffer.putBinary(bytesVal);
        }
        else if(colObject == null)
        {
        	pStmtDA.m_ResultBuffer.putNull();
        }
        else
        {
            jdam.trace(m_tmHandle, ip.UL_TM_INFO, "ip_process_column_res_val(). Class Name:<" + colObject.getClass() + ">\n");
        }
        return iRetCode;
    }


    public int ip_process_column_val(XXX_STMT_DA pStmtDA,long  hrow,long hcol,int iColNum)
    {
        int    iRetCode = 0;
        Object colObject = m_JdbcUtil.getColumnObject(pStmtDA.rs,iColNum + 1);

        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "ip_process_column_val() Called.>\n");

        if(colObject instanceof java.lang.Integer)
        {
            iRetCode = jdam.damex_addIntColValToRow(pStmtDA.dam_hstmt, hrow, hcol,
                m_JdbcUtil.getIntColumnValue(pStmtDA.rs,iColNum+1),16);
        }
        else if(colObject instanceof java.lang.String)
        {
            iRetCode = jdam.damex_addCharColValToRow(pStmtDA.dam_hstmt, hrow, hcol,
                m_JdbcUtil.getColumnValue(pStmtDA.rs, iColNum+1),XO_NTS);
        }
        else if(colObject instanceof java.math.BigDecimal)
        {
            iRetCode = jdam.damex_addCharColValToRow(pStmtDA.dam_hstmt, hrow, hcol,
                (new BigDecimal(m_JdbcUtil.getColumnValue(pStmtDA.rs, iColNum+1)).toPlainString()),XO_NTS);
        }
        else if(colObject instanceof java.lang.Float)
        {
            iRetCode = jdam.damex_addFloatColValToRow(pStmtDA.dam_hstmt, hrow, hcol,
                m_JdbcUtil.getFloatColumnValue(pStmtDA.rs, iColNum+1),16);
        }
        else if(colObject instanceof java.lang.Double)
        {
            iRetCode = jdam.damex_addDoubleColValToRow(pStmtDA.dam_hstmt, hrow, hcol,
                m_JdbcUtil.getDoubleColumnValue(pStmtDA.rs, iColNum+1),16);
        }
        else if(colObject instanceof java.lang.Boolean)
        {
            iRetCode = jdam.damex_addBitColValToRow(pStmtDA.dam_hstmt, hrow, hcol,
                m_JdbcUtil.getBooleanColumnValue(pStmtDA.rs, iColNum+1),1);
        }
        else if(colObject instanceof java.sql.Date)
        {
            Date val = m_JdbcUtil.getDateColumnValue(pStmtDA.rs, iColNum+1);
            xo_tm date = new xo_tm(val.getYear()+1900,val.getMonth(),val.getDate());
            iRetCode = jdam.damex_addTimeStampColValToRow(pStmtDA.dam_hstmt, hrow, hcol,date,16);
        }
        else if(colObject instanceof java.sql.Time)
        {
            Time val = m_JdbcUtil.getTimeColumnValue(pStmtDA.rs, iColNum+1);
            xo_tm time = new xo_tm(val.getHours(),val.getMinutes(),val.getSeconds(),0);
            iRetCode = jdam.damex_addTimeStampColValToRow(pStmtDA.dam_hstmt, hrow, hcol,time,16);
        }
        else if(colObject instanceof Timestamp)
        {
            Timestamp val = m_JdbcUtil.getTimeStampColumnValue(pStmtDA.rs, iColNum+1);
            xo_tm timestamp = new xo_tm(val.getYear()+1900,val.getMonth(),val.getDate(),val.getHours(),val.getMinutes(),val.getSeconds());
            iRetCode = jdam.damex_addTimeStampColValToRow(pStmtDA.dam_hstmt, hrow, hcol,timestamp,16);
        }
        return iRetCode;
    }

	public int ip_process_column_res_val(XXX_STMT_DA pStmtDA, long hrow,
			int iColNum) {
		int iRetCode = 0;
		Object colObject = m_JdbcUtil.getColumnObject(pStmtDA.rs, iColNum + 1);

		int colTypeXOType = MapJdbcTypeToXoType(m_JdbcUtil.getColumnType(pStmtDA.rs, iColNum + 1));

		jdam.trace(m_tmHandle, ip.UL_TM_INFO,
				"ip_process_column_res_val() Called\n");

		if (colObject != null) {
			jdam.trace(
					m_tmHandle,
					ip.UL_TM_INFO,
					"ip_process_column_res_val(). Column Name:<"
							+ m_JdbcUtil.getColumnName(pStmtDA.rs, iColNum + 1)
							+ ">. Object:<" + colObject.getClass() + ">\n");
		}

		if (colObject == null) {
			iRetCode = jdam.damex_addCharResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum, null, XO_NULL_DATA);
			return iRetCode;
		}


		switch (colTypeXOType) {
		case ip.XO_TYPE_INTEGER: {
			iRetCode = jdam.damex_addIntResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum,
					m_JdbcUtil.getIntColumnValue(pStmtDA.rs, iColNum + 1), 16);
		}
			break;

		case XO_TYPE_BIGINT: {
			iRetCode = jdam.damex_addBigIntResValToRow(
					pStmtDA.dam_hstmt,
					hrow,
					iColNum,
					(long) m_JdbcUtil.getBigDecimalColumnValue(pStmtDA.rs,
							iColNum + 1).longValue(), 24);

		}
			break;

		case ip.XO_TYPE_SMALLINT:
		case ip.XO_TYPE_TINYINT: {
			iRetCode = jdam
					.damex_addIntResValToRow(pStmtDA.dam_hstmt, hrow, iColNum,
							m_JdbcUtil.getShortColumnValue(pStmtDA.rs,
									iColNum + 1), 16);
		}
			break;

		case XO_TYPE_CHAR:
		case XO_TYPE_VARCHAR:
		case XO_TYPE_WCHAR:
		case XO_TYPE_WVARCHAR: {
			iRetCode = jdam.damex_addWCharResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum, colObject.toString(), XO_NTS);
		}
			break;

		case XO_TYPE_DECIMAL:
		case XO_TYPE_NUMERIC: {
			iRetCode = jdam.damex_addWCharResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum,
					(new BigDecimal(colObject.toString())).toPlainString(),
					XO_NTS);
		}
			break;

		case XO_TYPE_REAL:
		case XO_TYPE_FLOAT: {
			iRetCode = jdam
					.damex_addFloatResValToRow(pStmtDA.dam_hstmt, hrow,
							iColNum, m_JdbcUtil.getFloatColumnValue(pStmtDA.rs,
									iColNum + 1), 16);
		}
			break;

		case XO_TYPE_DOUBLE: {
			iRetCode = jdam.damex_addDoubleResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum,
					m_JdbcUtil.getDoubleColumnValue(pStmtDA.rs, iColNum + 1),
					16);
		}
			break;

		case XO_TYPE_BIT: {
			iRetCode = jdam.damex_addBitResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum,
					m_JdbcUtil.getBooleanColumnValue(pStmtDA.rs, iColNum + 1),
					1);
		}
			break;

		case XO_TYPE_DATE: {
			Date val = m_JdbcUtil.getDateColumnValue(pStmtDA.rs, iColNum + 1);
			xo_tm date = new xo_tm(val.getYear() + 1900, val.getMonth(),
					val.getDate());
			iRetCode = jdam.damex_addTimeStampResValToRow(pStmtDA.dam_hstmt,
					hrow, iColNum, date, 16);
		}
			break;

		case XO_TYPE_TIME: {
			Time val = m_JdbcUtil.getTimeColumnValue(pStmtDA.rs, iColNum + 1);
			xo_tm time = new xo_tm(val.getHours(), val.getMinutes(),
					val.getSeconds(), 0);
			iRetCode = jdam.damex_addTimeStampResValToRow(pStmtDA.dam_hstmt,
					hrow, iColNum, time, 16);
		}
			break;

		case XO_TYPE_TIMESTAMP: {
			Timestamp val = m_JdbcUtil.getTimeStampColumnValue(pStmtDA.rs,
					iColNum + 1);
			xo_tm timestamp = new xo_tm(val.getYear() + 1900, val.getMonth(),
					val.getDate(), val.getHours(), val.getMinutes(),
					val.getSeconds());
			iRetCode = jdam.damex_addTimeStampResValToRow(pStmtDA.dam_hstmt,
					hrow, iColNum, timestamp, 16);
		}
			break;

		case XO_TYPE_LONGVARBINARY: {
			iRetCode = passjdbc_blob_data(pStmtDA, hrow, iColNum);
		}
			break;

		case XO_TYPE_LONGVARCHAR:
		case XO_TYPE_WLONGVARCHAR: {
			iRetCode = passjdbc_clob_data(pStmtDA, hrow, iColNum);
		}
			break;

		case XO_TYPE_BINARY:
		case XO_TYPE_VARBINARY: {
			iRetCode = jdam.damex_addBinaryResValToRow(pStmtDA.dam_hstmt, hrow,
					iColNum, (byte[]) colObject,
					(int) Array.getLength((byte[]) colObject));
		}
			break;

		default: {
			jdam.trace(
					m_tmHandle,
					ip.UL_TM_INFO,
					"ip_process_column_res_val(). Class Name:<"
							+ colObject.getClass() + ">\n");
		}
		}

		return iRetCode;
	}

    /************************************************************************
     Function:       passjdbc_blob_data()
     Description:    Add the binary data to the result row
     Return:         DAM_SUCESS on success
     DAM_FAILURE on error
     ************************************************************************/
    int             passjdbc_blob_data(XXX_STMT_DA pStmtDA,long  hrow,int iColNum)
    {
        InputStream pSource = null;
        byte[]  pBuffer;
        long    nBufferSize;
        long    nAmtRead;

        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "passjdbc_blob_data() Called\n");
        
        try
        {            
            Blob val = m_JdbcUtil.getBlobColumnValue(pStmtDA.rs, iColNum+1);
            pSource = val.getBinaryStream();
            
            if (pSource == null) 
            {
                /* adda null value */                
                jdam.damex_addBinaryResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, null, XO_NULL_DATA);                
                return DAM_SUCCESS;
            }

            /* allocate a buffer */
            pBuffer = new byte[FILEIO_MAX_BUFFER_SIZE];
            nBufferSize = FILEIO_MAX_BUFFER_SIZE;

            nAmtRead = pSource.read(pBuffer, 0, (int)nBufferSize);
            while (nAmtRead > 0) 
            {
                jdam.damex_addBinaryResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, pBuffer, (int) nAmtRead);                                
                nAmtRead = pSource.read(pBuffer, 0,(int) nBufferSize);
            }

            /* Close the stream */
            pSource.close();
        }
        catch(Exception e)
        { 
            jdam.damex_addBinaryResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, null, XO_NULL_DATA);                
            return DAM_SUCCESS;

        }
        return DAM_SUCCESS;
    }

    /************************************************************************
     Function:       passjdbc_clob_data()
     Description:    Add the binary data to the result row
     Return:         DAM_SUCESS on success
     DAM_FAILURE on error
     ************************************************************************/
    int             passjdbc_clob_data(XXX_STMT_DA pStmtDA,long  hrow,int iColNum)
    {
        Reader pSource = null;
        char[]  pBuffer;
        long    nBufferSize;
        long    nAmtRead;
        String  pData;

        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "passjdbc_clob_data() Called\n");

        try
        {            
            Clob val = m_JdbcUtil.getClobColumnValue(pStmtDA.rs, iColNum+1);
            pSource = val.getCharacterStream();
            
            if (pSource == null) 
            {
                jdam.trace(m_tmHandle, ip.UL_TM_INFO, "passjdbc_clob_data() add Null data.\n");
                /* adda null value */                
                jdam.damex_addCharResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, null, XO_NULL_DATA);                
                return DAM_SUCCESS;
            }

            /* allocate a buffer */
            pBuffer = new char[FILEIO_MAX_BUFFER_SIZE];
            nBufferSize = FILEIO_MAX_BUFFER_SIZE;

            nAmtRead = pSource.read(pBuffer, 0, (int)nBufferSize);            
            
            /* Empty Stream. Add Null */
            if (nAmtRead <= 0) 
            {                
                jdam.damex_addCharResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, null, XO_NULL_DATA);                
            }

            while (nAmtRead > 0) 
            {                
                pData = new String(pBuffer,0,(int)nAmtRead);
                if(nAmtRead > 0)
                    pData = pData.concat(String.copyValueOf(pBuffer,0,(int)nAmtRead));
                jdam.damex_addCharResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, pData, (int) nAmtRead);                                
                nAmtRead = pSource.read(pBuffer, 0,(int) nBufferSize);
            }

            /* Close the stream */
            pSource.close();
        }
        catch(Exception e)
        { 
            jdam.trace(m_tmHandle, ip.UL_TM_INFO, "passjdbc_clob_data().Exception" + e.toString() + "\n");
            jdam.trace(m_tmHandle, ip.UL_TM_INFO, "passjdbc_clob_data() add Null data.\n");
            jdam.damex_addBinaryResValToRow(pStmtDA.dam_hstmt, hrow, iColNum, null, XO_NULL_DATA);                
            return DAM_SUCCESS;

        }
        return DAM_SUCCESS;
    }

    boolean    passjdbc_clob_data(XXX_STMT_DA pStmtDA,int iColNum,StringBuffer pData)
    {
        Reader pSource = null;
        char[]  pBuffer;
        long    nBufferSize;
        long    nAmtRead;
        
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "passjdbc_clob_data() Called\n");

        try
        {            
            Clob val = m_JdbcUtil.getClobColumnValue(pStmtDA.rs, iColNum+1);
            pSource = val.getCharacterStream();
            
            if (pSource == null) 
            {
                return false;
            }

            /* allocate a buffer */
            pBuffer = new char[FILEIO_MAX_BUFFER_SIZE];
            nBufferSize = FILEIO_MAX_BUFFER_SIZE;

            nAmtRead = pSource.read(pBuffer, 0, (int)nBufferSize);            
            
            /* Empty Stream. Add Null */
            if (nAmtRead <= 0) 
            {                
                return false;
            }

            while (nAmtRead > 0) 
            {                
                pData.append(pBuffer,0,(int)nAmtRead);
                nAmtRead = pSource.read(pBuffer, 0,(int) nBufferSize);
            }

            /* Close the stream */
            pSource.close();
        }
        catch(Exception e)
        { 
            return false;

        }
        return true;
    }

    public int getStmtIndex()
    {
        for (int i=0; i < m_stmtDA.length; i++) {
            if(m_stmtDA[i] == null)
                return i;
        }
        return -1;
    }

    public String  xxx_map_type_to_name(int iType)
    {
        switch (iType) 
        {
            case ip.XO_TYPE_CHAR:return "CHAR";
            case ip.XO_TYPE_VARCHAR:return "VARCHAR";
            case ip.XO_TYPE_LONGVARCHAR: return "LONGVARCHAR";
            case ip.XO_TYPE_NUMERIC:return "NUMERIC";
            case ip.XO_TYPE_DECIMAL:return "DECIMAL";
            case ip.XO_TYPE_INTEGER:return "INTEGER";
            case ip.XO_TYPE_SMALLINT:return "SMALLINT";
            case ip.XO_TYPE_DOUBLE:return "DOUBLE";
            case ip.XO_TYPE_BIGINT: return "BIGINT";
            case ip.XO_TYPE_FLOAT: return "FLOAT";
            case ip.XO_TYPE_REAL: return "REAL";
            case ip.XO_TYPE_WCHAR: return "WCAHR";
            case ip.XO_TYPE_WVARCHAR: return "WVARCHAR";
            case ip.XO_TYPE_WLONGVARCHAR: return "WLONGVARCHAR";
            case ip.XO_TYPE_DATE:  return "DATE";
            case 91:  return "DATE";  /* XO_TYPE_DATE_TYPE */
            case ip.XO_TYPE_TIME: return "TIME";
            case 92: return "TIME";  /* XO_TYPE_TIME_TYPE */
            case ip.XO_TYPE_TIMESTAMP: return "TIMESTAMP";
            case 93: return "TIMESTAMP";    /* XO_TYPE_TIMESTAMP_TYPE */
            case ip.XO_TYPE_BINARY: return "BINARY";
            case ip.XO_TYPE_VARBINARY: return "VARBINARY";
            case ip.XO_TYPE_LONGVARBINARY: return "LONGVARBINARY";
            case ip.XO_TYPE_TINYINT: return "TINYINT";
            case ip.XO_TYPE_BIT: return "BIT";
        }
        return "ERROR";
    }

    /* helper methods */
    public int generateCatalogList(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generateCatalogList() called\n");
        ResultSet rs = m_JdbcUtil.getCatalogs();
        if(rs == null)
        {
	    return IP_FAILURE;
        }

        schemaobj_table TableObj = new schemaobj_table();
        while(true)
        {
            try
            {
                if(!rs.next())
                    break;
                TableObj.SetObjInfo(m_JdbcUtil.getColumnValue(rs,1),null,null,null,null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj, TableObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateCatalogList():Exception in execute(): "+ e + "\n");
            }
        }
        return IP_SUCCESS;
    }

    public int generateSchemasList(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generateSchemasList() called\n");
        ResultSet rs = m_JdbcUtil.getSchemas();
        if(rs == null)
        {
            return IP_FAILURE;
        }

        schemaobj_table TableObj = new schemaobj_table();
        while(true)
        {
            try
            {
                if(!rs.next())
                    break;
                TableObj.SetObjInfo(null,m_JdbcUtil.getColumnValue(rs,1),null,null,null,null,null,null);
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj, TableObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateSchemasList():Exception in execute(): "+ e + "\n");
            }
        }
        return IP_SUCCESS;
    }

    public int generateTableList(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generateTableList() called\n");
        String      pCatalog = null;
        String      pSchema = null;
        String      pTable = null;
        if(pSearchObj != null) 
        {
            schemaobj_table searchTableObj = (schemaobj_table)pSearchObj;
            pCatalog = searchTableObj.getTableQualifier();
            pSchema = searchTableObj.getTableOwner();
            pTable = searchTableObj.getTableName();
        
        }
        ResultSet rs = m_JdbcUtil.getTables(pCatalog,pSchema,pTable,null);
        if(rs == null)
        {
            return IP_FAILURE;
        }
        schemaobj_table TableObj = new schemaobj_table();
        while(true)
        {
	    String sTableType;
            try
            {
                if(!rs.next())
                    break;
		sTableType = (0 == m_JdbcUtil.getColumnValue(rs,4).compareToIgnoreCase("VIEW")) ? "TABLE" :  m_JdbcUtil.getColumnValue(rs,4);
		
                TableObj.SetObjInfo(m_JdbcUtil.getColumnValue(rs,1),m_JdbcUtil.getColumnValue(rs,2),
                    m_JdbcUtil.getColumnValue(rs,3),sTableType,
                    null,null,null,m_JdbcUtil.getColumnValue(rs,5));
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj, TableObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateTableList():Exception in execute(): "+ e + "\n");
            }
        }
        return IP_SUCCESS;
    }

    public int generateColumnList(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generateColumnList() called\n");
        String      pCatalog = null;
        String      pSchema = null;
        String      pTable = null;
        String      pColumn = null;
        
        if(pSearchObj != null) 
        {
            schemaobj_column searchColObj = (schemaobj_column)pSearchObj;
            pCatalog = searchColObj.getTableQualifier();
            pSchema = searchColObj.getTableOwner();
            pTable = searchColObj.getTableName();
            pColumn = searchColObj.getColumnName();
        }

        ResultSet rs = m_JdbcUtil.getColumns(pCatalog,pSchema,pTable,pColumn);
        if(rs == null)
        {
            return IP_FAILURE;
        }
        schemaobj_column ColumnObj = new schemaobj_column();
        while(true)
        {
            try
            {
                if(!rs.next())
                    break;
                ColumnObj.SetObjInfo(m_JdbcUtil.getColumnValue(rs,1),m_JdbcUtil.getColumnValue(rs,2),
                    m_JdbcUtil.getColumnValue(rs,3), m_JdbcUtil.getColumnValue(rs,4),
                    (short)MapJdbcTypeToXoType(m_JdbcUtil.getIntColumnValue(rs,5)),
                    MapJdbcTypeToXoTypeName(m_JdbcUtil.getIntColumnValue(rs,5)),m_JdbcUtil.getIntColumnValue(rs,7),
                    m_JdbcUtil.getIntColumnValue(rs,7),(short)m_JdbcUtil.getIntColumnValue(rs,10),(short)m_JdbcUtil.getIntColumnValue(rs,9),
                    (short)m_JdbcUtil.getIntColumnValue(rs,11),(short)DAMOBJ_NOTSET,null,null,
                    (short)SQL_PC_NOT_PSEUDO,(short)0,m_JdbcUtil.getColumnValue(rs,12));
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,ColumnObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateColumnList():Exception in execute(): "+ e + "\n");
        
            }
        }
        return IP_SUCCESS;
    }

    public int generatePrimaryKeys(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generatePrimaryKeys() called\n");
        String      pCatalog = null;
        String      pSchema = null;
        String      pTable = null;

        if(pSearchObj != null) 
        {
            schemaobj_pkey pSearchPkeyObj = (schemaobj_pkey)pSearchObj;
            pCatalog = pSearchPkeyObj.getPKTableQualifier();
            pSchema = pSearchPkeyObj.getPKTableOwner();
            pTable = pSearchPkeyObj.getPKTableName();
        }
        else
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generatePrimaryKeys():Specified parameter values not supported \n");
            return IP_FAILURE;
        }

        ResultSet rs = m_JdbcUtil.getPrimaryKeys(pCatalog,pSchema,pTable);
        if(rs == null)
        {
            return IP_FAILURE;
        }
        schemaobj_pkey PkeyObj = new schemaobj_pkey();
        while(true)
        {
            try
            {
                if(!rs.next())
                    break;
                PkeyObj.SetObjInfo(m_JdbcUtil.getColumnValue(rs,1),m_JdbcUtil.getColumnValue(rs,2),
                    m_JdbcUtil.getColumnValue(rs,3), m_JdbcUtil.getColumnValue(rs,4),
                    (short)m_JdbcUtil.getShortColumnValue(rs,5), m_JdbcUtil.getColumnValue(rs,6));
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,PkeyObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generatePrimaryKeys():Exception in execute(): "+ e + "\n");
        
            }
        }
        return IP_SUCCESS;
    }

    public int generateForeignKeys(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generateForeignKeys() called\n");
        String primaryCatalog = null;
        String primarySchema  = null;
        String primaryTable   = null;
        String foreignCatalog = null;
        String foreignSchema  = null;
        String foreignTable   = null;

        if(pSearchObj != null) 
        {
            schemaobj_fkey pSearchFkeyObj = (schemaobj_fkey)pSearchObj;
            primaryCatalog = pSearchFkeyObj.getPKTableQualifier();
            primarySchema = pSearchFkeyObj.getPKTableOwner();
            primaryTable = pSearchFkeyObj.getPKTableName();
            foreignCatalog = pSearchFkeyObj.getFKTableQualifier();
            foreignSchema = pSearchFkeyObj.getFKTableOwner();
            foreignTable = pSearchFkeyObj.getFKTableName();
        }
        else
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateForeignKeys():Specified parameter values not supported \n");
            return IP_FAILURE;
        }

        ResultSet rs = m_JdbcUtil.getForeignKeys(primaryCatalog,primarySchema,primaryTable,
                                                 foreignCatalog,foreignSchema,foreignTable);
        if(rs == null)
        {
            return IP_FAILURE;
        }
        schemaobj_fkey FkeyObj = new schemaobj_fkey();
        while(true)
        {
            try
            {
                if(!rs.next())
                    break;
                FkeyObj.SetObjInfo(m_JdbcUtil.getColumnValue(rs,1),m_JdbcUtil.getColumnValue(rs,2),
                    m_JdbcUtil.getColumnValue(rs,3), m_JdbcUtil.getColumnValue(rs,4),
                    m_JdbcUtil.getColumnValue(rs,5), m_JdbcUtil.getColumnValue(rs,6),
                    m_JdbcUtil.getColumnValue(rs,7), m_JdbcUtil.getColumnValue(rs,8),
                    (short)m_JdbcUtil.getShortColumnValue(rs,9),(short)m_JdbcUtil.getShortColumnValue(rs,10),
                    (short)m_JdbcUtil.getShortColumnValue(rs,11),m_JdbcUtil.getColumnValue(rs,12),
                    m_JdbcUtil.getColumnValue(rs,13));
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,FkeyObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateForeignKeys():Exception in execute(): "+ e + "\n");
        
            }
        }
        return IP_SUCCESS;
    }

    public int generateStatistics(long pMemTree,int iType, long pList, Object pSearchObj)
    {
        jdam.trace(m_tmHandle, ip.UL_TM_INFO, "generateStatistics() called\n");
        String      pCatalog = null;
        String      pSchema = null;
        String      pTable = null;        
        
        if(pSearchObj != null) 
        {
            schemaobj_stat pSearchStatObj = (schemaobj_stat)pSearchObj;
            pCatalog = pSearchStatObj.getTableQualifier();            
            pSchema = pSearchStatObj.getTableOwner();            
            pTable = pSearchStatObj.getTableName();            
        }
        else
        {
            jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateStatistics():Specified parameter values not supported \n");
            return IP_FAILURE;
        }

        ResultSet rs = m_JdbcUtil.getIndexInfo(pCatalog,pSchema,pTable,false,true);
        if(rs == null)
        {
            return IP_FAILURE;
        }
        schemaobj_stat StatObj = new schemaobj_stat();
        while(true)
        {
            try
            {
                if(!rs.next())
                    break;
                StatObj.SetObjInfo(m_JdbcUtil.getColumnValue(rs,1),m_JdbcUtil.getColumnValue(rs,2),
                    m_JdbcUtil.getColumnValue(rs,3), (short)(m_JdbcUtil.getBooleanColumnValue(rs,4) ? 1 : 0),
                    m_JdbcUtil.getColumnValue(rs,5), m_JdbcUtil.getColumnValue(rs,6),
                    (short)m_JdbcUtil.getShortColumnValue(rs,7), (short)m_JdbcUtil.getShortColumnValue(rs,8),
                    m_JdbcUtil.getColumnValue(rs,9),m_JdbcUtil.getColumnValue(rs,10),
                    m_JdbcUtil.getIntColumnValue(rs,11),m_JdbcUtil.getIntColumnValue(rs,12),
                    m_JdbcUtil.getColumnValue(rs,13));
                jdam.dam_add_schemaobj(pMemTree,iType,pList,pSearchObj,StatObj);
            }
            catch(Exception e)
            {
                jdam.trace(m_tmHandle, ip.UL_TM_ERRORS, "generateStatistics():Exception in execute(): "+ e + "\n");
        
            }
        }
        return IP_SUCCESS;
    }

    private int MapJdbcTypeToXoType(int iJdbcType)
    {
        int iXoType = -1;

        /* map Jdbc data type to XO type */
        switch(iJdbcType)
        {
            case Types.SMALLINT: iXoType = XO_TYPE_SMALLINT; break;
            case Types.INTEGER:  iXoType = XO_TYPE_INTEGER; break;
            case Types.BIGINT: iXoType = XO_TYPE_BIGINT; break;
            case Types.DECIMAL:  iXoType = XO_TYPE_DECIMAL; break;
            case Types.FLOAT:    iXoType = XO_TYPE_FLOAT; break;
            case Types.REAL:     iXoType = XO_TYPE_REAL; break;
            case Types.DOUBLE:   iXoType = XO_TYPE_DOUBLE; break;
            case Types.NUMERIC:  iXoType = XO_TYPE_NUMERIC; break;
            case Types.CHAR:     iXoType = XO_TYPE_WCHAR; break;
            case Types.VARCHAR:        iXoType = XO_TYPE_WVARCHAR; break;
            case Types.LONGVARCHAR:    iXoType = XO_TYPE_WLONGVARCHAR; break;
            case Types.DATE:     iXoType = XO_TYPE_DATE; break;
            case Types.TIME:     iXoType = XO_TYPE_TIME; break;
            case Types.TIMESTAMP:     iXoType = XO_TYPE_TIMESTAMP; break;
            case Types.BINARY:   iXoType = XO_TYPE_BINARY; break;
            case Types.BIT:      iXoType = XO_TYPE_BIT; break;
            case Types.VARBINARY: iXoType = XO_TYPE_VARBINARY; break;
            case Types.LONGVARBINARY: iXoType = XO_TYPE_LONGVARBINARY; break;
            case Types.CLOB:     iXoType = XO_TYPE_LONGVARCHAR; break;
            case Types.BLOB:     iXoType = XO_TYPE_LONGVARBINARY; break;
            case Types.TINYINT: iXoType = XO_TYPE_TINYINT; break;             
            case Types.OTHER: iXoType = XO_TYPE_WVARCHAR; break;
            default: iXoType = XO_TYPE_CHAR;
        }

        return iXoType;
    }

    private String MapJdbcTypeToXoTypeName(int iJdbcType)
    {        
        /* map Jdbc data type to XO type Name */
        switch(iJdbcType)
        {
            case Types.SMALLINT:        return "SMALLINT";
            case Types.INTEGER:         return "INTEGER";
            case Types.BIGINT:          return "BIGINT";
            case Types.DECIMAL:         return "NUMERIC";
            case Types.FLOAT:           return "FLOAT";
            case Types.REAL:            return "REAL";
            case Types.DOUBLE:          return "DOUBLE";
            case Types.NUMERIC:         return "NUMERIC";
            case Types.CHAR:            return "CHAR";
            case Types.VARCHAR:         return "WVARCHAR";
            case Types.LONGVARCHAR:     return "WLONGVARCHAR";
            case Types.DATE:            return "DATE";
            case Types.TIME:            return "TIME";
            case Types.TIMESTAMP:       return "TIMESTAMP";
            case Types.BINARY:          return "BINARY";
            case Types.BIT:             return "BIT";
            case Types.VARBINARY:       return "VARBINARY";
            case Types.LONGVARBINARY:   return "LONGVARBINARY";
            case Types.CLOB:            return "LONGVARBINARY";
            case Types.BLOB:            return "LONGVARBINARY";
            case Types.TINYINT:         return "TINYINT"; 
            case Types.OTHER:           return "WLONGVARCHAR";
            default:                    return "CHAR";
        }             
    }
} /* Class dbpassjdbc */

/********************************************************************************************
    Class:          XXX_STMT_DA
    Description:    Store Statement level information

*********************************************************************************************/

class XXX_STMT_DA {
    long                dam_hstmt;      /* DAM handle to the statement */

    int                 iType;          /* Type of the query */
    int                 iFetchSize;
    int                 iRowCount;
    int                 iColCount;
    int                 iTableCount;
    int                 iCurTableNum;   /* used to identify if column belongs to current table */


    java.sql.ResultSet  rs;

    ResultBuffer	m_ResultBuffer;
    
    XXX_STMT_DA()
    {
        rs = null;
	m_ResultBuffer = null;
    }
};
/* Procedure Descriptor Area */
class PASSJDBC_PROC_DA {
    long                pMemTree;
    long                dam_hstmt;      /* DAM handle to the statement */

    StringBuffer        sQualifier; 
    StringBuffer        sOwner; 
    StringBuffer        sProcName;      /* Name of the Procedure being queried */
    StringBuffer        sUserData; 

    int         iTotalResultSets;   /* Number of result sets that should be returned */
    int         iCurResultSetNum;   /* current result set being processed */
    int         iItems;             /* number of rows in each result set */
    int         iCurItems;          /* number of rows in each result set */
    long        iFetchSize;     /* records to return in cursor mode */

    PASSJDBC_PROC_DA()
    {
        sQualifier = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
        sOwner = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
        sProcName = new StringBuffer(ip.DAM_MAX_ID_LEN+1);
        sUserData = new StringBuffer(ip.DAM_MAX_ID_LEN+1);

        iTotalResultSets = 0;
        iCurResultSetNum = 0;
        iItems = 0;
        iCurItems = 0;
        iFetchSize = 0;
    }
};
