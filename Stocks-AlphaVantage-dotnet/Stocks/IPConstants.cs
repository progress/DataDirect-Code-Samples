using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace oanet.damip
{
    public  class IPConstants
    {
        
        public Dictionary<string, ColumnInfo> defaultColumnTypeInfo = new Dictionary<string, ColumnInfo>();
        public void setColumnTypes()
        {
            defaultColumnTypeInfo.Add("CHAR", new ColumnInfo((short)1, "CHAR", 255, 255, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("NUMERIC", new ColumnInfo((short)2, "NUMERIC", 130, 127, (short)-1, (short)6, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("DECIMAL", new ColumnInfo((short)3, "DECIMAL", 130, 127, (short)-1, (short)6, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("INTEGER", new ColumnInfo((short)4, "INTEGER", 4, 10, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("SMALLINT", new ColumnInfo((short)5, "SMALLINT", 2, 5, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("FLOAT", new ColumnInfo((short)6, "FLOAT", 8, 15, (short)-1, (short)3, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("REAL", new ColumnInfo((short)7, "REAL", 4, 7, (short)-1, (short)3, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("DOUBLE", new ColumnInfo((short)8, "DOUBLE", 8, 15, (short)-1, (short)3, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("DATE", new ColumnInfo((short)9, "DATE", 6, 10, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("TIME", new ColumnInfo((short)10, "TIME", 6, 8, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("TIMESTAMP", new ColumnInfo((short)11, "TIMESTAMP", 16, 19, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("VARCHAR", new ColumnInfo((short)12, "VARCHAR", 8000, 8000, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("DATE_TYPE", new ColumnInfo((short)91, "DATE_TYPE", 6, 10, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("TIME_TYPE", new ColumnInfo((short)92, "TIME_TYPE", 6, 8, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("TIMESTAMP_TYPE", new ColumnInfo((short)93, "TIMESTAMP_TYPE", 16, 19, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("NULL", new ColumnInfo((short)0, "NULL", 0, 0, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("LONGVARCHAR", new ColumnInfo((short)-1, "LONGVARCHAR", 1000000, 1000000, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("BINARY", new ColumnInfo((short)-2, "BINARY", 255, 255, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("VARBINARY", new ColumnInfo((short)-3, "VARBINARY", 8000, 8000, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("LONGVARBINARY", new ColumnInfo((short)-4, "LONGVARBINARY", 1000000, 1000000, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("BIGINT", new ColumnInfo((short)-5, "BIGINT", 19, 19, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("TINYINT", new ColumnInfo((short)-6, "TINYINT", 1, 3, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("BIT", new ColumnInfo((short)-7, "BIT", 1, 1, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("WCHAR", new ColumnInfo((short)-8, "WCHAR", 510, 255, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("WVARCHAR", new ColumnInfo((short)-9, "WVARCHAR", 16000, 8000, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
            defaultColumnTypeInfo.Add("WLONGVARCHAR", new ColumnInfo((short)-10, "WLONGVARCHAR", 2000000, 1000000, (short)-1, (short)0, (short)1, (short)-1, null, null, (short)1, (short)0, null));
        }

        /* Support array */
        public int[] ip_support_array = new int[]
                    {
                        0,
                        1, /* IP_SUPPORT_SELECT */
                        0, /* IP_SUPPORT_INSERT */
                        0, /* IP_SUPPORT_UPDATE */
                        0, /* IP_SUPPORT_DELETE */
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
                        0, /* IP_SUPPORT_CREATE_TABLE */
                        0, /* IP_SUPPORT_DROP_TABLE */
                        0, /* IP_SUPPORT_CREATE_INDEX */
                        0, /* IP_SUPPORT_DROP_INDEX */
                        0, /* IP_SUPPORT_PROCEDURE */
                        0, /* IP_SUPPORT_CREATE_VIEW */
                        0, /* IP_SUPPORT_DROP_VIEW */
                        0, /* IP_SUPPORT_QUERY_VIEW */
                        0, /* IP_SUPPORT_CREATE_USER */
                        0, /* IP_SUPPORT_DROP_USER */
                        0, /* IP_SUPPORT_CREATE_ROLE */
                        0, /* IP_SUPPORT_DROP_ROLE */
                        0, /* IP_SUPPORT_GRANT */
                        0, /* IP_SUPPORT_REVOKE */
                        0,  /* IP_SUPPORT_PASSTHROUGH_QUERY */
                        0,  /* IP_SUPPORT_NATIVE_COMMAND */
                        0,  /* IP_SUPPORT_ALTER_TABLE */
                        0,  /* IP_SUPPORT_BLOCK_JOIN */
                        0,  /* IP_SUPPORT_XA */
                        0,  /* IP_SUPPORT_QUERY_MODE_SELECTION */
                        0,  /* IP_SUPPORT_VALIDATE_SCHEMAOBJECTS_IN_USE */
                        1,  /* IP_SUPPORT_UNICODE_INFO */
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
    }
}
