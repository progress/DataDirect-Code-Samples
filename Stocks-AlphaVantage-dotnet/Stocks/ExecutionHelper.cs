using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using openaccess;
using oanet.sql;
using Newtonsoft.Json.Linq;

namespace oanet.damip
{
    class ExecutionHelper
    {
        public int executeQueryWithOptimization(long dam_hstmt, ref long hindex, ref long hset_of_condlist, int iStmtType, out long piNumResRows, string customProperty)
        {
            long hcur_condlist;
            long hcond;
            piNumResRows = 0;
            int iStatus;
            
            Dictionary<string, string> eqlConditions = new Dictionary<string, string>();
            Dictionary<string, string> greaterConditions = new Dictionary<string, string>();
            Dictionary<string, string> smallerConditions = new Dictionary<string, string>();

            /* get the conditions on index columns */
            hcur_condlist = ndam.dam_getFirstCondList(hset_of_condlist);

            while (hcur_condlist != 0)
            {
                int iLeftOp = 0;
                int iLeftValType = 0, iLeftDataLen;
                string pLeftData;
                int unusedFlag;

                /* Each condition list will have only one condition, since its a single column index */
                //Get the First Condition
                hcond = ndam.dam_getFirstCond(dam_hstmt, hcur_condlist);

                //Get the name of the column
                long colInCond = ndam.dam_getColInCond(hcond);
                StringBuilder sbColName = new StringBuilder();
                ndam.dam_describeCol(colInCond, out unusedFlag, sbColName, out unusedFlag, out unusedFlag);

                /* Get the value and the operator */
                pLeftData = (string)ndam.dam_describeCondEx(dam_hstmt, hcond, ipc.DAM_COND_PART_LEFT, out iLeftOp, out iLeftValType, out iLeftDataLen, out iStatus);

                //Categorize for pushing down the API if it supports.
                if (iLeftOp == ipc.SQL_OP_EQUAL)
                {
                    eqlConditions.Add(sbColName.ToString().ToLower(), pLeftData);
                }
                //Greater and smaller than are not needed for Alphavantage as the API doesn't support these operators, they are only for demonstration in the sample.
                else if (iLeftOp == ipc.SQL_OP_GREATER)
                {
                    greaterConditions.Add(sbColName.ToString().ToLower(), pLeftData);
                }
                else if (iLeftOp == ipc.SQL_OP_SMALLER)
                {
                    smallerConditions.Add(sbColName.ToString().ToLower(), pLeftData);
                }

                //Get the next Cond List
                hcur_condlist = ndam.dam_getNextCondList(hset_of_condlist);

            }

            APIHelper apiHelper = new APIHelper(eqlConditions, customProperty);
            JObject jsonData = apiHelper.getDataFromAPI();
            addRowsToOA(jsonData, dam_hstmt, eqlConditions, out piNumResRows);
            return ipc.IP_SUCCESS;
        }

        public int addRowsToOA(JObject jsonData, long dam_hstmt, Dictionary<string, string> eqlConditions, out long piNumResRows)
        {
            Dictionary<string, string> metadata = jsonData.First.First.ToObject<Dictionary<string, string>>();
            string information = null, symbol = null, lastRefreshed = null, interval = null, ouputSize = null, timeZone = null;
            int iRetCode = 0;
            piNumResRows = 0;
            long hInformation = 0, hFunction = 0, hSymbol = 0, hLastRefreshed = 0, hInterval = 0, hOutputSize = 0, hTimeZone = 0, hTimeStampRecorded = 0, hOpen = 0, hHigh = 0, hLow = 0, hClose = 0, hVolume = 0;

            
            //Get Column handles for all the columns in that table.
            hInformation = ndam.dam_getCol(dam_hstmt, "Information");
            hFunction = ndam.dam_getCol(dam_hstmt, "Function");
            hSymbol = ndam.dam_getCol(dam_hstmt, "Symbol");
            hLastRefreshed = ndam.dam_getCol(dam_hstmt, "LastRefreshed");
            hInterval = ndam.dam_getCol(dam_hstmt, "Interval");
            hOutputSize = ndam.dam_getCol(dam_hstmt, "OutputSize");
            hTimeZone = ndam.dam_getCol(dam_hstmt, "TimeZone");
            hTimeStampRecorded = ndam.dam_getCol(dam_hstmt, "Timestamp_Recorded");
            hOpen = ndam.dam_getCol(dam_hstmt, "Open");
            hHigh = ndam.dam_getCol(dam_hstmt, "High");
            hLow = ndam.dam_getCol(dam_hstmt, "Low");
            hClose = ndam.dam_getCol(dam_hstmt, "Close");
            hVolume = ndam.dam_getCol(dam_hstmt, "Volume");

            foreach (KeyValuePair<string, string> entry in metadata)
            {
                if (entry.Key.Contains("Information"))
                {
                    information = entry.Value;
                }
                else if (entry.Key.Contains("Symbol"))
                {
                    symbol = entry.Value;
                }
                else if (entry.Key.Contains("Last"))
                {
                    lastRefreshed = entry.Value;
                }
                else if (entry.Key.Contains("Interval"))
                {
                    interval = entry.Value;
                }
                else if (entry.Key.Contains("Time"))
                {
                    timeZone = entry.Value;
                }
                else if (entry.Key.Contains("Output"))
                {
                    ouputSize = entry.Value.Contains("Full") ? eqlConditions["outputsize"] : entry.Value; 
                }

            }

            string function = eqlConditions["function"];


            JObject timeseriesData = (JObject)jsonData.Last.First;
            //var record = timeseriesData.First;
            foreach(JProperty property in timeseriesData.Properties())
            {
                string name = property.Name;
               
            }


            foreach (JProperty property in timeseriesData.Properties())
            {
                long hrow = 0;
                DateTime timestampRecorded = DateTime.Parse(property.Name);
                Dictionary<string, string> stockprices = property.Value.ToObject<Dictionary<string, string>>();
                string open = null, high=null, low=null, close=null, volume=null;

                foreach (KeyValuePair<string, string> entry in stockprices)
                {
                    if (entry.Key.Contains("open"))
                    {
                        open = entry.Value;
                    }
                    else if (entry.Key.Contains("high"))
                    {
                        high = entry.Value;
                    }
                    else if (entry.Key.Contains("low"))
                    {
                        low = entry.Value;
                    }
                    else if (entry.Key.Contains("close"))
                    {
                        close = entry.Value;
                    }
                    else if (entry.Key.Contains("volume"))
                    {
                        volume = entry.Value;
                    }
                    
                }

                //Handle Allow Row
                hrow = ndam.damex_allocRow(dam_hstmt);

                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hInformation, information, (information != null) ? ipc.XO_NTS : ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding Information to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding Information to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hFunction, function, (function != null) ? ipc.XO_NTS : ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding function to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding function to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hSymbol, symbol, (symbol != null) ? ipc.XO_NTS : ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding Symbol to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding Symbol to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hLastRefreshed, lastRefreshed, (lastRefreshed != null) ? ipc.XO_NTS : ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding Last Refreshed to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding Last Refreshed to Row");
                    return iRetCode; /* return on error */
                }


                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hInterval, interval, (interval!=null) ? ipc.XO_NTS: ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding Interval to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding Interval to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hOutputSize, ouputSize, (ouputSize != null) ? ipc.XO_NTS : ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding Output Size to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding Output Size to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addCharValToRow(dam_hstmt, hrow, hTimeZone, timeZone, (timeZone != null) ? ipc.XO_NTS : ipc.XO_NULL_DATA);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error Adding TimeZone to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error Adding TimeZone to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addTimeStampValToRow(dam_hstmt, hrow, hTimeStampRecorded, ref timestampRecorded, 0);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error adding Timestop recorded to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error adding Timestop recorded to Row");
                    return iRetCode; /* return on error */
                }


                iRetCode = ndam.dam_addDoubleValToRow(dam_hstmt, hrow, hOpen, Convert.ToDouble(open), 0);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error adding open to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error adding open to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addDoubleValToRow(dam_hstmt, hrow, hClose, Convert.ToDouble(close), 0);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error adding close to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error adding close to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addDoubleValToRow(dam_hstmt, hrow, hHigh, Convert.ToDouble(high), 0);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error adding high to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error adding open to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addDoubleValToRow(dam_hstmt, hrow, hLow, Convert.ToDouble(low), 0);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error adding low to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error adding low to Row");
                    return iRetCode; /* return on error */
                }

                iRetCode = ndam.dam_addDoubleValToRow(dam_hstmt, hrow, hVolume, Convert.ToDouble(volume), 0);
                if (iRetCode != ipc.IP_SUCCESS)
                {
                    ndam.trace(dam_hstmt, ipc.UL_TM_ERRORS, "addRowsToOA: Error adding volume to Row\n");
                    ndam.dam_addError(0, dam_hstmt, ipc.DAM_IP_ERROR, 500, "addRowsToOA: Error adding volume to Row");
                    return iRetCode; /* return on error */
                }


                if(ndam.dam_isTargetRow(dam_hstmt, hrow) == ipc.DAM_TRUE)
                {
                    ndam.dam_addRowToTable(dam_hstmt, hrow);
                }

                piNumResRows++;

                
            }

            return ipc.DAM_SUCCESS;
        }
    }
}
