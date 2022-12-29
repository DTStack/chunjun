<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->
SqlTypeNameSpec SqlCustomTypeParse(Span s) :
{
String typeName = null;
Integer precision = null;
Integer scale = null;
String precisionSuffix = null;
Token t;
}
{
(
<BINARY_FLOAT> {typeName = OracleType.BINARY_FLOAT.name(); }
|
<BINARY_DOUBLE> {typeName = OracleType.BINARY_DOUBLE.name(); }
|
<LONG> {typeName = OracleType.LONG.name(); } [<RAW> {typeName = OracleType.LONG_RAW.getSourceName();}]
|
<NUMBER> {typeName = OracleType.NUMBER.name(); }
|
<RAW>{typeName = OracleType.RAW.name(); }
|
<VARCHAR>{ typeName = OracleType.VARCHAR.name(); }
|
<VARCHAR2>{ typeName = OracleType.VARCHAR.name(); }
|
<NVARCHAR2>{typeName = OracleType.VARCHAR2.name(); }
|
<ROWID>{typeName = OracleType.ROWID.name(); }
|
<UROWID>{typeName = OracleType.UROWID.name(); }
|
<CHAR>{typeName = OracleType.CHAR.name(); }
|
<NCHAR>{typeName = OracleType.NCHAR.name(); }
|
<CLOB>{typeName = OracleType.CLOB.name(); }
|
<NCLOB>{typeName = OracleType.NCLOB.name(); }
|
<BLOB>{typeName = OracleType.BLOB.name(); }
|
<BFILE>{typeName = OracleType.BFILE.name(); }
|
<JSON>{typeName = OracleType.JSON.name(); }
)
[
<LPAREN>
    (<NULL>|precision = UnsignedIntLiteral())
[<COMMA> scale = UnsignedIntLiteral()]
[
t = <CHAR>| t = <BYTE> { precisionSuffix = t.image; return new SqlSuffixTypeNameSpec(typeName, null, s.end(this), precision, precisionSuffix, scale); }
]
<RPAREN>
]
{return new SqlCustomTypeNameSpec( typeName, s.end(this), precision, null, false, false );}
}

SqlTypeNameSpec SqlNumberTypeParse(Span s) :
{
SqlTypeName sqlTypeName;
int precision = -1;
}
{
(
<SMALLINT> {sqlTypeName = SqlTypeName.SMALLINT; }
|
<INT> {sqlTypeName = SqlTypeName.INTEGER; }
|
<INTEGER> {sqlTypeName = SqlTypeName.INTEGER; }
|
<FLOAT> {sqlTypeName = SqlTypeName.FLOAT; }
|
<DOUBLE> {sqlTypeName = SqlTypeName.DOUBLE; }
)
precision = getSinglePs(-1)
{return new SqlNumberTypeNameSpec(sqlTypeName, precision, -1, false, false, s.end(this));}
}

SqlTypeNameSpec SqlTimestampTypeParse(Span s) :
{
Integer precision = null;
String typeName = null;
String typeNameSuffix = null;
}
{
{typeName = OracleType.TIMESTAMP.name();}
<LPAREN>
precision = UnsignedIntLiteral()
<RPAREN>
[<WITH> {typeNameSuffix = "WITH TIME ZONE";} [<LOCAL>{typeNameSuffix = "WITH LOCAL TIME ZONE";}] <TIME> <ZONE>]
{return new SqlSuffixTypeNameSpec(typeName, typeNameSuffix, s.end(this), precision, null, null);}
}

int getSinglePs(int defaultPrecision) :
{
}
{
[
<LPAREN>
{return UnsignedIntLiteral();}
<RPAREN>
]
{return defaultPrecision;}
}

SqlTypeNameSpec SqlIntervalTypeParse(Span s) :
{
Integer precision = null;
Integer scale = null;
String typeName = null;
String typeNameSuffix = null;
}
{
(
<YEAR>
{
typeName = "INTERVAL YEAR";
precision = getSinglePs(2);
typeNameSuffix = "TO MONTH";
}
<TO> <MONTH>
|
<DAY>
{
typeName = "INTERVAL DAY";
precision = getSinglePs(2);
typeNameSuffix = "TO SECOND";
scale = getSinglePs(6);
}
<TO> <SECOND>
)
{return new SqlSuffixTypeNameSpec(typeName, typeNameSuffix, s.end(this), precision, null, scale);}
}
