using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace oanet.damip
{
    public class ColumnInfo
    {
        public short uDataType;
        public string sTypeName;
        public int lCharMaxLength;
        public int lNumericPrecision;
        public short uNumericPrecisionRadix;
        public short uNumericScale;
        public short uNullable;
        public short uScope;
        public string sUserData;
        public string sOperatorSupport;
        public short uPseudoColumn;
        public short uColumnType;
        public string sRemarks;

        public ColumnInfo(short uDataType, string sTypeName,
                int lCharMaxLength, int lNumericPrecision,
                short uNumericPrecisionRadix, short uNumericScale,
                short uNullable, short uScope, string sUserData,
                string sOperatorSupport, short uPseudoColumn,
                short uColumnType, string sRemarks)
        {

            this.uDataType = uDataType;
            this.sTypeName = sTypeName;
            this.lCharMaxLength = lCharMaxLength;
            this.lNumericPrecision = lNumericPrecision;
            this.uNumericPrecisionRadix = uNumericPrecisionRadix;
            this.uNumericScale = uNumericScale;
            this.uNullable = uNullable;
            this.uScope = uScope;
            this.sUserData = sUserData;
            this.sOperatorSupport = sOperatorSupport;
            this.uPseudoColumn = uPseudoColumn;
            this.uColumnType = uColumnType;
            this.sRemarks = sRemarks;
        }


    }
}
