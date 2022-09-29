SqlTypeNameSpec SqlTypeNameNumberUnsigned() :
{
SqlTypeName sqlTypeName;
boolean isUnsigned = false;
boolean isZeroFill = false;
int precision = -1;
int scale = -1;
}
{
(
<INTEGER> {  sqlTypeName = SqlTypeName.INTEGER; }
    |
<TINYINT> {  sqlTypeName = SqlTypeName.TINYINT; }
    |
<SMALLINT> {  sqlTypeName = SqlTypeName.SMALLINT; }
    |
<BIGINT> {  sqlTypeName = SqlTypeName.BIGINT; }
    |
<FLOAT> {  sqlTypeName = SqlTypeName.FLOAT; }
    |
<DOUBLE> {  sqlTypeName = SqlTypeName.DOUBLE; }
    |
<DECIMAL> {  sqlTypeName = SqlTypeName.DECIMAL; }
)
[
<LPAREN>
    precision = UnsignedIntLiteral()
    [
    <COMMA>
    scale = UnsignedIntLiteral()
    ]
<RPAREN>
]
(
<UNSIGNED>{isUnsigned = true;}
    |
<ZEROFILL>{isZeroFill = true;}
)*{return new SqlNumberTypeNameSpec(sqlTypeName, precision, scale, isUnsigned, isZeroFill, getPos());}
}


SqlTypeNameSpec SqlMediumintTypeNameSpecParse() :
{
String typeName = null;
boolean isUnsigned = false;
boolean isZeroFill = false;
Integer precision = null;
Integer scale = null;
}
{
(
    <INT>{  typeName = MysqlType.INT.name(); }
        |
    <MEDIUMINT>{typeName = MysqlType.MEDIUMINT.name();}
        |
    <BIT>{typeName = MysqlType.BIT.name();}
        |
    <YEAR>{typeName = MysqlType.YEAR.name();}
        |
   <BLOB>{typeName = MysqlType.BLOB.name();}
       |
   <DATETIME>{typeName = MysqlType.DATETIME.name();}
       |
   <JSON>{typeName = MysqlType.JSON.name();}
       |
   <POINT>{typeName = MysqlType.POINT.name();}
       |
   <LINESTRING>{typeName = MysqlType.LINESTRING.name();}
       |
   <MULTIPOINT>{typeName = MysqlType.MULTIPOINT.name();}
       |
   <MULTILINESTRING>{typeName = MysqlType.MULTILINESTRING.name();}
       |
   <MULTIPOLYGON>{typeName = MysqlType.MULTIPOLYGON.name();}
       |
   <GEOMCOLLECTION>{typeName = MysqlType.GEOMCOLLECTION.name();}
       |
   <TEXT>{typeName = MysqlType.TEXT.name();}
       |
   <TINYBLOB>{typeName = MysqlType.TINYBLOB.name();}
       |
   <MEDIUMBLOB>{typeName = MysqlType.MEDIUMBLOB.name();}
       |
   <LONGBLOB>{typeName = MysqlType.LONGBLOB.name();}
       |
   <TINYTEXT>{typeName = MysqlType.TINYTEXT.name();}
       |
   <MEDIUMTEXT>{typeName = MysqlType.MEDIUMTEXT.name();}
       |
   <LONGTEXT>{typeName = MysqlType.LONGTEXT.name();}
       |
   <NUMERIC>{typeName = MysqlType.NUMERIC.name();}
)
[
<LPAREN>
precision = UnsignedIntLiteral()
[
<COMMA>
scale = UnsignedIntLiteral()
]
<RPAREN>
]
(
<UNSIGNED>{isUnsigned = true;}
|
<ZEROFILL>{isZeroFill = true;}
)*{return new SqlCustomTypeNameSpec(typeName, getPos(), precision, scale, isUnsigned, isZeroFill);}
}



SqlTypeNameSpec SqlSetTypeNameSpecParse() :
{
String typeName = null;
SqlNode sqlNode = null;
List<SqlNode> values = new ArrayList<SqlNode>();
}
{
(
<ENUM>{typeName = "enum";}
|
<SET>{typeName = "SET";}
)
<LPAREN>
    sqlNode = StringLiteral(){
    values.add(sqlNode);
    }
    (
    <COMMA> sqlNode = StringLiteral(){
        values.add(sqlNode);
        }
    )*
<RPAREN>{return new SqlSetTypeNameSpec(typeName, getPos(), values);}
}
