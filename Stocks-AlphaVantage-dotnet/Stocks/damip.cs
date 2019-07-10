/* damip.cs
 *
 * Copyright (c) 1995-2013 Progress Software Corporation. All Rights Reserved.
 *
 * Description:     Template DAM IP 
 *                  - is implemented in ".NET" 
 *                  - supports SELECT operations
 */

using System;

using openaccess;
using oanet.sql;
using System.Text;

namespace oanet.damip
{

    /* define the class damip to implement the sample IP */
    public class damip : oanet.sql.ip
    {
        private Int64 m_tmHandle = 0;
        IPConstants ipConstants;
        const string OA_CATALOG_NAME = "STOCKS";        /* SCHEMA */
        const string OA_USER_NAME = "OAUSER";        /* OAUSER */
        static string customProperty = null;

        public damip()
        {
            m_tmHandle = 0;
            ipConstants = new IPConstants();
            ipConstants.setColumnTypes();
        }

        public string ipGetInfo(int iInfoType)
        {
            string str = null;
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipGetInfo called\n");

            return str;
        }

        public int ipSetInfo(int iInfoType, string InfoVal)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipSetInfo called\n");
            return ipc.IP_SUCCESS;
        }

        public int ipGetSupport(int iSupportType)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipGetSupport called\n");
            return (ipConstants.ip_support_array[iSupportType]);
        }

        /*ipConnect is called immediately after an instance of this object is created. You should
         *perform any tasks related to connecting to your data source */
        public int ipConnect(Int64 tmHandle, Int64 dam_hdbc, string sDbName, string sUserName, string sPassword,
                            string sCurrentCatalog, string sIPProperties, string sIPCustomProperties)
        {
            /* Save the trace handle */
            m_tmHandle = tmHandle;
            customProperty = sIPCustomProperties;
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipConnect called\n");

            /* Code to connect to your data source source. */
            return ipc.IP_SUCCESS;
        }

        public int ipDisconnect(Int64 dam_hdbc)
        {   /* disconnect from the data source */
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipDisonnect called\n");
            return ipc.IP_SUCCESS;
        }

        public int ipStartTransaction(Int64 dam_hdbc)
        {
            /* start a new transaction */
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipStartTransaction called\n");
            return ipc.IP_SUCCESS;
        }

        public int ipEndTransaction(Int64 dam_hdbc, int iType)
        {
            /* end the transaction */
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipEndTransaction called\n");
            if (iType == ipc.DAM_COMMIT)
            {
            }
            else if (iType == ipc.DAM_ROLLBACK)
            {
            }
            return ipc.IP_SUCCESS;
        }

        public int ipExecute(Int64 dam_hstmt, int iStmtType, Int64 hSearchCol, out long piNumResRows)
        {
            long hindex = 0;
            long hset_of_condlist = 0;
            int pbpartialList;
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipExecute called\n");
            piNumResRows = 0;
            int iRetCode;

            StringBuilder tableName = new StringBuilder(ipc.DAM_MAX_ID_LEN + 1);

            if(iStmtType == ipc.DAM_SELECT)
            {
                //Get the table name on which query is being requested.
                ndam.dam_describeTable(dam_hstmt, null, null, tableName, null, null);

                


                /* get the where condition lists information and process the query*/
                //Documentation: http://media.datadirect.com/download/docs/openaccess/alloa/index.html#page/netref%2Fdam-getsetofconditionlistsex.html%23
                hset_of_condlist = ndam.dam_getSetOfConditionListsEx(dam_hstmt, ipc.SQL_SET_CONDLIST_INTERSECT, 0, out pbpartialList);
                

                /* use index information for optimized query processing - cases where NAME='Joe' type of 
                   conditions appear in the query*/
                if (hset_of_condlist != 0)
                {
                    ndam.trace(m_tmHandle, ipc.UL_TM_MAJOR_EV, "Found Where conditions\n");
                    ExecutionHelper executionHelper = new ExecutionHelper();
                    iRetCode = executionHelper.executeQueryWithOptimization(dam_hstmt, ref hindex, ref hset_of_condlist, iStmtType, out piNumResRows, customProperty);
                    ndam.dam_freeSetOfConditionList(hset_of_condlist); /* free the set of condition list */
                    if (iRetCode != ipc.IP_SUCCESS)
                    {   /* check for errors */
                        return ipc.IP_FAILURE;
                    }
                }
                else
                {
                    /* non-optimized query processing -- cases where no WHERE clause or conditions on columns
                       other than NAME. Full Table Scan*/

                    // Alphavantage API needs atleast 2 parameters - function and symbol. So, if no where condition is provided, we will have to throw an error as we can't build and execute the request successfully.
                    ndam.trace(m_tmHandle, ipc.UL_TM_MAJOR_EV, "No where condition found\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 403, "Please provide function and symbol in your where clause");
                    return ipc.IP_FAILURE; 

                   
                }
            }
            return ipc.IP_SUCCESS;
        }

        public int ipSchema(Int64 dam_hdbc, Int64 pMemTree, int iType, Int64 pList, object pSearchObj)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipSchema called\n");
            switch (iType)
            {
                case ipc.DAMOBJ_TYPE_CATALOG:
                    {
                        schemaobj_table TableObj = new schemaobj_table(OA_CATALOG_NAME, null, null, null, null, null, null, null);

                        ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);
                    }
                    break;
                case ipc.DAMOBJ_TYPE_SCHEMA:
                    {
                        schemaobj_table TableObj = new schemaobj_table();

                        TableObj.SetObjInfo(null, "SYSTEM", null, null, null, null, null, null);
                        ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);

                        TableObj.SetObjInfo(null, OA_USER_NAME, null, null, null, null, null, null);
                        ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);
                    }
                    break;
                case ipc.DAMOBJ_TYPE_TABLETYPE:
                    {
                        schemaobj_table TableObj = new schemaobj_table();

                        TableObj.SetObjInfo(null, null, null, "SYSTEM TABLE", null, null, null, null);
                        ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);

                        TableObj.SetObjInfo(null, null, null, "TABLE", null, null, null, null);
                        ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);

                        TableObj.SetObjInfo(null, null, null, "VIEW", null, null, null, null);
                        ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);
                    }

                    break;

                case ipc.DAMOBJ_TYPE_TABLE:
                    {
                        schemaobj_table pSearchTableObj = (schemaobj_table)pSearchObj;

                        if (pSearchTableObj != null)
                        {
                            ndam.trace(m_tmHandle, ipc.UL_TM_MAJOR_EV, "Dynamic Schema  of table:<" + pSearchTableObj.getTableQualifier() + "." + pSearchTableObj.getTableOwner() + "." + pSearchTableObj.getTableName() + "> is being requested\n");
                        }
                        else
                        {
                            ndam.trace(m_tmHandle, ipc.UL_TM_MAJOR_EV, "Dynamic Schema for all tables is being requested\n");
                        }

                        //add new table to openaccess
                        if (IsMatchingTable(pSearchTableObj, OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES"))
                        {
                            schemaobj_table TableObj = new schemaobj_table();
                            TableObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "TABLE", null, null, null, "Timeseries Table");
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, TableObj);
                        }
                    }
                    break;

                case ipc.DAMOBJ_TYPE_COLUMN:
                    {
                        schemaobj_column pSearchColumnObj = (schemaobj_column)pSearchObj;

                        if (pSearchColumnObj != null)
                        {
                            ndam.trace(m_tmHandle, ipc.UL_TM_MAJOR_EV, "Dynamic Schema for column <" + pSearchColumnObj.getColumnName() + "> of table:<" + pSearchColumnObj.getTableQualifier() + "." + pSearchColumnObj.getTableOwner() + "." + pSearchColumnObj.getTableName() + "> is being requested\n");
                        }
                        else
                        {
                            ndam.trace(m_tmHandle, ipc.UL_TM_MAJOR_EV, "Dynamic Schema for all columns of all tables is being requested\n");
                        }

                        //add columns to the table TIMESERIES
                        if (IsMatchingColumn(pSearchColumnObj, OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES"))
                        {
                            schemaobj_column columnObj = new schemaobj_column();

                            ColumnInfo columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Information", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Function", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Symbol", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "LastRefreshed", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Interval", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "OutputSize", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["VARCHAR"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "TimeZone", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["TIMESTAMP"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Timestamp_Recorded", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["DOUBLE"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Open", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["DOUBLE"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "High", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["DOUBLE"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Low", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["DOUBLE"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Close", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                            columnInfo = ipConstants.defaultColumnTypeInfo["NUMERIC"];
                            columnObj.SetObjInfo(OA_CATALOG_NAME, OA_USER_NAME, "TIMESERIES", "Volume", columnInfo.uDataType, columnInfo.sTypeName, columnInfo.lCharMaxLength, columnInfo.lNumericPrecision, columnInfo.uNumericPrecisionRadix, columnInfo.uNumericScale, columnInfo.uNullable, columnInfo.uScope, columnInfo.sUserData, columnInfo.sOperatorSupport, columnInfo.uPseudoColumn, columnInfo.uColumnType, columnInfo.sRemarks);
                            ndam.dam_add_schemaobj(pMemTree, iType, pList, pSearchObj, columnObj);

                        }

                    }
                    break;

                case ipc.DAMOBJ_TYPE_STAT:
                    break;
                case ipc.DAMOBJ_TYPE_FKEY:
                    break;
                case ipc.DAMOBJ_TYPE_PKEY:
                    break;
                case ipc.DAMOBJ_TYPE_PROC:
                    break;
                case ipc.DAMOBJ_TYPE_PROC_COLUMN:
                    break;
                default:
                    break;
            }
            return ipc.IP_SUCCESS;

        }

        public int ipDDL(Int64 dam_hstmt, int iStmtType, out long piNumResRows)
        {
            piNumResRows = 0;
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipDDL called\n");
            return ipc.IP_FAILURE;
        }

        public int ipProcedure(Int64 dam_hstmt, int iType, out long piNumResRows)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipProcedure called\n");
            piNumResRows = 0;
            return ipc.IP_FAILURE;
        }

        public int ipDCL(Int64 dam_hstmt, int iStmtType, out long piNumResRows)
        {
            piNumResRows = 0;
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipDCL called\n");
            return ipc.IP_FAILURE;
        }

        public int ipPrivilege(int iStmtType, string pcUserName, string pcCatalog, string pcSchema, string pcObjName)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipPrivilege called\n");
            return ipc.IP_FAILURE;
        }

        public int ipNative(Int64 dam_hstmt, int iCommandOption, string sCommand, out long piNumResRows)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipNative called\n");
            piNumResRows = 0;
            return ipc.IP_FAILURE;
        }

        public int ipSchemaEx(Int64 dam_hstmt, Int64 pMemTree, int iType, Int64 pList, object pSearchObj)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipSchemaEx called\n");
            return ipc.IP_FAILURE;
        }

        public int ipProcedureDynamic(Int64 dam_hstmt, int iType, out long piNumResRows)
        {
            ndam.trace(m_tmHandle, ipc.UL_TM_F_TRACE, "ipProcedureDynamic called\n");
            piNumResRows = 0;
            return ipc.IP_FAILURE;
        }

        private bool IsMatchingTable(schemaobj_table pSearchObj, string table_qualifier, string table_owner, string table_name)
        {
            if (pSearchObj == null) return true;

            /* match the search pattern */
            if ((pSearchObj.getTableQualifier() != null) && pSearchObj.getTableQualifier().ToUpper().CompareTo(table_qualifier.ToUpper()) != 0)
                return false;
            if ((pSearchObj.getTableOwner() != null) && pSearchObj.getTableOwner().ToUpper().CompareTo(table_owner.ToUpper()) != 0)
                return false;
            if ((pSearchObj.getTableName() != null) && pSearchObj.getTableName().ToUpper().CompareTo(table_name.ToUpper()) != 0)
                return false;

            return true;
        }

        private bool IsMatchingColumn(schemaobj_column pSearchObj,
           string table_qualifier,
           string table_owner,
           string table_name)
        {
            if (pSearchObj == null) return true;

            /* match the search pattern */
            if ((pSearchObj.getTableQualifier() != null) && pSearchObj.getTableQualifier().ToUpper().CompareTo(table_qualifier.ToUpper()) != 0)
                return false;
            if ((pSearchObj.getTableOwner() != null) && pSearchObj.getTableOwner().ToUpper().CompareTo(table_owner.ToUpper()) != 0)
                return false;
            if ((pSearchObj.getTableName() != null) && pSearchObj.getTableName().ToUpper().CompareTo(table_name.ToUpper()) != 0)
                return false;

            return true;
        }
    }
}
