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

<#--    token-->
<DEFAULT, DQID, BTID, BQID, BQHID> TOKEN :
{
< VISIBLE: "VISIBLE" >
|
< INVISIBLE: "INVISIBLE" >
|
< AUTO_INCREMENT: "AUTO_INCREMENT" >
|
< COLUMN_FORMAT: "COLUMN_FORMAT" >
|
< STORAGE: "STORAGE" >
|
< STORED: "STORED" >
<#--|-->
<#--< INDEX: "INDEX" >-->
|
< BTREE: "BTREE" >
|
< HASH: "HASH" >
|
< FULLTEXT: "FULLTEXT" >
|
< SPATIAL: "SPATIAL" >
|
< ENGINE_ATTRIBUTE: "ENGINE_ATTRIBUTE" >
|
< SECONDARY_ENGINE_ATTRIBUTE: "SECONDARY_ENGINE_ATTRIBUTE" >
|
< PARSER: "PARSER" >
|
< KEY_BLOCK_SIZE: "KEY_BLOCK_SIZE" >
|
< AUTOEXTEND_SIZE: "AUTOEXTEND_SIZE" >
|
< AVG_ROW_LENGTH: "AVG_ROW_LENGTH" >
|
< CHECKSUM: "CHECKSUM" >
|
< COMPRESSION: "COMPRESSION" >
|
< DIRECTORY: "DIRECTORY" >
|
< DELAY_KEY_WRITE: "DELAY_KEY_WRITE" >
|
< ENCRYPTION: "ENCRYPTION" >
|
< ENGINE: "ENGINE" >
|
< INSERT_METHOD: "INSERT_METHOD" >
|
< MAX_ROWS: "MAX_ROWS" >
|
< MIN_ROWS: "MIN_ROWS" >
|
< PACK_KEYS: "PACK_KEYS" >
|
< PASSWORD: "PASSWORD" >
|
< ROW_FORMAT: "ROW_FORMAT" >
|
< STATS_AUTO_RECALC: "STATS_AUTO_RECALC" >
|
< STATS_PERSISTENT: "STATS_PERSISTENT" >
|
< STATS_SAMPLE_PAGES: "STATS_SAMPLE_PAGES" >
|
< DISK: "DISK" >
|
< MEMORY: "MEMORY" >
|
< CHARSET: "CHARSET" >
|
< ALGORITHM: "ALGORITHM" >
|
< CHANGE: "CHANGE" >

}

SqlCharStringLiteral createStringLiteral(String s, SqlParserPos pos) :
{
}
{
{ return SqlLiteral.createCharString(SqlParserUtil.parseString(s), pos); }
}



/**
* Parses a "IF NOT EXISTS" option, default is false.
*/
boolean IfNotExistsOpt() :
{
}
{

(
LOOKAHEAD(3)
<IF> <NOT> <EXISTS> { return true; }
|
{ return false; }
)

}


boolean IfExistsOpt() :
{
}
{
LOOKAHEAD(2)
<IF> <EXISTS> { return true; }
        |
        { return false; }
        }


        SqlCreate SqlCreateDatabase(Span s, boolean replace) :
        {
        boolean isDataBase = false;
        SqlIdentifier databaseName;
        boolean ifNotExists = false;
        SqlNodeList propertyList;
        }
        {
       ( <DATABASE> {isDataBase = true;} | <SCHEMA> {isDataBase = false;})
                    ifNotExists = IfNotExistsOpt()
                    databaseName = CompoundIdentifier()
                    propertyList = DataBaseProperty(){
                    return new SqlCreateDataBase(getPos(), ifNotExists, isDataBase,databaseName,propertyList);
                    }
        }


SqlCreate SqlCreateTable(Span s, boolean replace) :
{
final SqlParserPos startPos = s.pos();
// 声明变量
final boolean ifNotExists;
final SqlIdentifier name;
SqlIdentifier originalName = null;
SqlNode query = null;
SqlLiteral handleDuplicateUniqueKey = null;
boolean likeTable = false;
boolean isTemporary = false;
SqlNodeList columnList = SqlNodeList.EMPTY;
SqlNodeList indexList = SqlNodeList.EMPTY;
SqlNodeList constraintList = SqlNodeList.EMPTY;
SqlNodeList checkConstraintList = SqlNodeList.EMPTY;
SqlNodeList tableOptions = SqlNodeList.EMPTY;
SqlParserPos pos = startPos;
}
{
[<TEMPORARY> { isTemporary = true; } ]
<TABLE> ifNotExists = IfNotExistsOpt() name = CompoundIdentifier()

[
<LPAREN> {
pos = getPos();
TableCreationContext ctx = new TableCreationContext();
}
CreateDefinition(ctx)
(
<COMMA> CreateDefinition(ctx)
)*
{
pos = pos.plus(getPos());
if(CollectionUtils.isNotEmpty(ctx.columnList)){
columnList =new SqlNodeList(ctx.columnList, pos);
}
if(CollectionUtils.isNotEmpty(ctx.indexList)){
indexList = new SqlNodeList(ctx.indexList, pos);
}
if(CollectionUtils.isNotEmpty(ctx.constraintList)){
constraintList = new SqlNodeList(ctx.constraintList, pos);
}
if(CollectionUtils.isNotEmpty(ctx.checkConstraintList)){
checkConstraintList = new SqlNodeList(ctx.checkConstraintList, pos);
}
}
<RPAREN>
]
tableOptions = TableProperty()

<#--(-->
<#--[<REPLACE> {handleDuplicateUniqueKey = SqlMysqlConstraintEnable.REPLACE.symbol(getPos());}]-->
<#--[<IGNORE> {handleDuplicateUniqueKey = SqlMysqlConstraintEnable.IGNORE.symbol(getPos());}]-->
<#--[<AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) | query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)]-->
<#--)-->
[<LIKE>  {likeTable = true; originalName = CompoundIdentifier();}]
{
return new SqlCreateTable(s.end(this),replace,isTemporary,name,ifNotExists,likeTable,originalName, columnList,indexList,constraintList,checkConstraintList,tableOptions);
}
}

SqlCreate SqlCreateIndex(Span s, boolean replace) :
{
SqlLiteral specialType = null;
SqlIdentifier indexName;
SqlNode indexType = null;
SqlIdentifier tableName;
SqlNodeList keyPartNodeList = null;
SqlNode indexOption = null;
SqlLiteral algorithmOption = null;
SqlLiteral lockOption = null;
}
{
[
<UNIQUE>{specialType =  SqlLiteral.createSymbol(IndexType.UNIQUE,getPos()); }
|
<FULLTEXT>{specialType = SqlLiteral.createSymbol(IndexType.FULLTEXT,getPos()) ;}
    |
<SPATIAL>{specialType =  SqlLiteral.createSymbol(IndexType.SPATIAL,getPos()); }
]
<INDEX>
    indexName = SimpleIdentifier()
    [indexType = SqlIndexType()]
<ON>
    tableName = CompoundIdentifier()
<LPAREN> {
        SqlParserPos pos = getPos();
        List<SqlNode> keyPartList = new ArrayList();
            }
            KeyPart(keyPartList)
            (
            <COMMA> KeyPart(keyPartList)
                )*
                {
                pos = pos.plus(getPos());
                keyPartNodeList = new SqlNodeList(keyPartList, pos);
                }
<RPAREN>
    indexOption = SqlIndexOption()

    (
    (
    <ALGORITHM> [<EQ>]
            (
            <DEFAULT_>{ algorithmOption = AlterTableAlgorithmType.DEFAULT.symbol(getPos()); }
            |
            <INPLACE>{ algorithmOption = AlterTableAlgorithmType.INPLACE.symbol(getPos()); }
            |
            <COPY>{ algorithmOption = AlterTableAlgorithmType.COPY.symbol(getPos()); }
            )
    )

    |

    (<LOCK> [<EQ>] (
                        <DEFAULT_>{ lockOption = AlterTableLockTypeEnum.DEFAULT.symbol(getPos()); }
                        |
                        <NONE>{ lockOption = AlterTableLockTypeEnum.NONE.symbol(getPos()); }
                        |
                        <SHARED>{ lockOption = AlterTableLockTypeEnum.SHARED.symbol(getPos()); }
                        |
                        <EXCLUSIVE>{ lockOption = AlterTableLockTypeEnum.EXCLUSIVE.symbol(getPos()); }
                   )
    )
)*{ return new SqlCreateIndex(getPos(), new SqlIndex(getPos(), indexName, specialType, indexType, keyPartNodeList, indexOption, algorithmOption, lockOption),tableName);}

}

void CreateDefinition(TableCreationContext context):
{

}
{
(
<#--                    LOOKAHEAD(3)-->
TableIndex(context)
|
TableColumn(context)
|
TableConstraint(context)
)
}

void TableColumn(TableCreationContext context) :
{
SqlIdentifier name = null;
SqlDataTypeSpec type = null;
SqlNode defaultValue = null ;
SqlNode e = null;

SqlLiteral nullAble = null;
SqlLiteral visiable = null;
SqlLiteral autoIncreMent = null;
SqlLiteral uniqueKey = null;
SqlLiteral primary = null;
SqlNode comment = null ;
SqlNode collation = null;
SqlLiteral columnFormat = null;
SqlNode engineAttribute = null;
SqlNode secondaryEngineAttribute = null;
SqlLiteral storage = null;
SqlLiteral stored = null;
SqlNode sqlCheckConstraint = null;
SqlIdentifier checkConstraintName = null;

SqlNode referenceDefinition = null;
}
{
name = SimpleIdentifier()
type = DataType()

(
(LOOKAHEAD(2)
<NOT> <NULL>  { nullAble = SqlMysqlConstraintEnable.NOT_NULLABLE.symbol(getPos()); }
|
<NULL> { nullAble = SqlMysqlConstraintEnable.NULLABLE.symbol(getPos()); }
)
|
<DEFAULT_> defaultValue = Expression(ExprContext.ACCEPT_SUB_QUERY){
    if(defaultValue instanceof SqlLiteral ){
    SqlLiteral sqliteral = (SqlLiteral) defaultValue;
    if(sqliteral.getTypeName().equals(SqlTypeName.NULL)){
    defaultValue = null;
    }
    }
    } [<ON> <UPDATE> Expression(ExprContext.ACCEPT_SUB_QUERY)]
|
(
<VISIBLE> { visiable = SqlMysqlConstraintEnable.VISIBLE.symbol(getPos()); }
|
<INVISIBLE> { visiable = SqlMysqlConstraintEnable.INVISIBLE.symbol(getPos()); }
)
|
<AUTO_INCREMENT> { autoIncreMent = SqlMysqlConstraintEnable.AUTO_INCREMENT.symbol(getPos()); }
|
(
[<GENERATED><ALWAYS>]
<AS><LPAREN>e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
)
|
(
<VIRTUAL> { stored = StoredTypeEnum.VIRTUAL.symbol(getPos()); }
|
<STORED> { stored = StoredTypeEnum.STORED.symbol(getPos()); }
)

|
(
<UNIQUE>[<KEY>] { uniqueKey = SqlMysqlConstraintEnable.UNIQUE.symbol(getPos()); }

|
<PRIMARY>  <KEY> { primary = SqlMysqlConstraintEnable.PRIMARY.symbol(getPos()); }

|
<KEY> { primary = SqlMysqlConstraintEnable.PRIMARY.symbol(getPos()); }
)

|
<COMMENT> <QUOTED_STRING> {
comment = createStringLiteral(token.image, getPos());
}
|
<COLLATE> [<EQ>]{
        collation = SqlStringLiteralBySimpleIdentifier();
        }
|
(
 <COLUMN_FORMAT> (
           <FIXED> {columnFormat = SqlStringLiteral(token.image, getPos());}
               |
           <DYNAMIC> {columnFormat = SqlStringLiteral(token.image, getPos());}
               |
           <DEFAULT_> {columnFormat = SqlStringLiteral(token.image, getPos());}
     )
)
|
<ENGINE_ATTRIBUTE> [<EQ>] {engineAttribute = StringLiteral();}
|
<SECONDARY_ENGINE_ATTRIBUTE>  [<EQ>] {secondaryEngineAttribute = StringLiteral();}
|
(
<STORAGE>(
     <DISK>{ storage = StorageTypeEnum.DISK.symbol(getPos());}
   |
     <MEMORY>{storage = StorageTypeEnum.MEMORY.symbol(getPos());}
    )
)
)*
[referenceDefinition = ReferenceDefinition()]
[
<#--    alter table c1 int after c0 如果不加LOOKAHEAD 会匹配进下面CheckDefinition 所以需要继续查找三个 是check的才会进入-->
 LOOKAHEAD(3)
[<CONSTRAINT> [checkConstraintName = SimpleIdentifier()]]
sqlCheckConstraint = CheckDefinition(checkConstraintName)
]
{
context.columnList.add(new SqlGeneralColumn(getPos(),name,type,e,defaultValue,nullAble,visiable,autoIncreMent,uniqueKey,primary,comment,collation,columnFormat,engineAttribute,secondaryEngineAttribute,storage,stored,referenceDefinition,sqlCheckConstraint));
}
}

void TableIndex(TableCreationContext context) :
{
SqlLiteral specialType = null;
}
{
Index(context,specialType)

|
<FULLTEXT>{specialType = SqlLiteral.createSymbol(IndexType.FULLTEXT,getPos()) ;}  SpecialIndex(context,specialType)
|
<SPATIAL>{specialType =  SqlLiteral.createSymbol(IndexType.SPATIAL,getPos()); }  SpecialIndex(context,specialType)
}


void Index(TableCreationContext context, SqlLiteral specialType):
{
boolean isIndex = true;
SqlIdentifier name = null;

SqlNodeList keyPartNodeList = null;
SqlNode sqlIndexOption = null;
SqlNode sqlIndexType = null;
}
{
( <INDEX> {isIndex = true;} | <KEY> {isIndex = false;})
[name =  SimpleIdentifier()]
[sqlIndexType = SqlIndexType()]
<LPAREN> {
SqlParserPos pos = getPos();
List<SqlNode> keyPartList = new ArrayList();
}
KeyPart(keyPartList)
(
<COMMA> KeyPart(keyPartList)
)*
{
pos = pos.plus(getPos());
    keyPartNodeList = new SqlNodeList(keyPartList, pos);
}
<RPAREN>
    sqlIndexOption = SqlIndexOption(){
if(isIndex){
context.indexList.add(new SqlIndex(getPos(),name,specialType,sqlIndexType,keyPartNodeList,sqlIndexOption));
}else{
context.indexList.add(new SqlKey(getPos(),name,specialType,sqlIndexType,keyPartNodeList,sqlIndexOption));
}
}


}

void SpecialIndex(TableCreationContext context, SqlLiteral specialType):
{
boolean isIndex = true;
SqlIdentifier name = null;

SqlLiteral indexType = null;
SqlNodeList keyPartNodeList = null;
SqlNode indexOption = null;
}
{
[ <INDEX> {isIndex = true;} | <KEY> {isIndex = false;}]

[name = SimpleIdentifier()]
[
<USING>
<BTREE> {indexType = SqlLiteral.createSymbol(IndexStorageType.BTREE, getPos()); }
|
<HASH> {indexType = SqlLiteral.createSymbol(IndexStorageType.HASH, getPos()); }
]
<LPAREN> {
SqlParserPos pos = getPos();
List<SqlNode> keyPartList = new ArrayList();
}
KeyPart(keyPartList)
(
<COMMA> KeyPart(keyPartList)
)*
{
pos = pos.plus(getPos());
    keyPartNodeList = new SqlNodeList(keyPartList, pos);
}
<RPAREN>
indexOption = SqlIndexOption()
    {
if(isIndex){
context.indexList.add(new SqlIndex(getPos(),name,specialType,indexType,keyPartNodeList,indexOption));
}else{
context.indexList.add(new SqlKey(getPos(),name,specialType,indexType,keyPartNodeList,indexOption));
}
}


}

void KeyPart( List<SqlNode> keyPartList):
{
SqlNode name;
SqlNode length = null;
SqlLiteral storageOrder = null;
}
{
name = Expression(ExprContext.ACCEPT_SUB_QUERY)

[<ASC>{storageOrder = SqlLiteral.createSymbol(OrderEnum.ASC,getPos()) ;}
|
<DESC>{ storageOrder = SqlLiteral.createSymbol(OrderEnum.DESC,getPos());}]
{
keyPartList.add(new KeyPart(getPos(),name, length,storageOrder));
}
}


void TableConstraint (TableCreationContext context) :
{
SqlIdentifier name = null;
SqlNode constraint = null;
SqlNode checkConstraint = null;
}
{

[
<CONSTRAINT>
]
[
name = SimpleIdentifier()
]


(
<PRIMARY> {constraint = TablePrimaryConstraint(name);}
|
<UNIQUE> {constraint =  TableUniqueConstraint(name);}
|
<FOREIGN> {constraint = TableForeignConstraint(name);}
|
checkConstraint = CheckDefinition(name)

)

{
if(constraint != null){
context.constraintList.add(constraint);
}
if(checkConstraint != null ){
context.checkConstraintList.add(checkConstraint);
}
}
}

SqlNode CheckDefinition(SqlIdentifier name):
{
SqlNode expr =null;
SqlLiteral  enforcement;
}{
<CHECK>
expr = Expression(ExprContext.ACCEPT_SUB_QUERY)
enforcement = ConstraintEnforcement()
{
return new SqlCheckConstraint(getPos(),name,expr,enforcement);
}
}

SqlLiteral ConstraintEnforcement() :
{
SqlLiteral enforcement = null;
}
{
[
<ENFORCED> {
enforcement = SqlConstraintEnforcement.ENFORCED.symbol(getPos());
}
|
<NOT> <ENFORCED> {
enforcement = SqlConstraintEnforcement.NOT_ENFORCED.symbol(getPos());
}
]
{
return enforcement;
}
}


SqlNode TablePrimaryConstraint(SqlIdentifier name):
{
SqlIdentifier indexNmae = null;
SqlLiteral uniqueSpec = null;
SqlNode indexType = null ;
SqlNodeList keyPartList = SqlNodeList.EMPTY ;

SqlNode referenceDefinition = null;
SqlNode indexOption = null;
}
{
<KEY>{uniqueSpec = SqlConstraintSpec.PRIMARY_KEY.symbol(getPos());}
[indexType = SqlIndexType()]
<LPAREN> {
SqlParserPos pos = getPos();
List<SqlNode> keyParts = new ArrayList();
}
KeyPart(keyParts)
(
<COMMA> KeyPart(keyParts)
)*
<RPAREN>
{
pos = pos.plus(getPos());
keyPartList = new SqlNodeList(keyParts, pos);
}
indexOption = SqlIndexOption()
{
return new SqlTableConstraint(getPos(),name,uniqueSpec,null,keyPartList,indexType,referenceDefinition,indexOption);
}
}


SqlNode TableUniqueConstraint(SqlIdentifier name):
{
SqlIdentifier indexNmae = null;
SqlLiteral uniqueSpec =  SqlConstraintSpec.UNIQUE.symbol(getPos());
SqlNode sqlIndexType = null ;
SqlNodeList keyPartList = SqlNodeList.EMPTY ;

SqlNode referenceDefinition = null;
SqlNode sqlIndexOption = null;
}
{

[<KEY>{uniqueSpec = SqlConstraintSpec.UNIQUE_KEY.symbol(getPos());}
|
<INDEX>{uniqueSpec = SqlConstraintSpec.UNIQUE_INDEX.symbol(getPos());}
]
[indexNmae = SimpleIdentifier()]
[sqlIndexType = SqlIndexType()]
<LPAREN> {
SqlParserPos pos = getPos();
List<SqlNode> keyParts = new ArrayList();
}
KeyPart(keyParts)
(
<COMMA> KeyPart(keyParts)
)*
<RPAREN>
{
pos = pos.plus(getPos());
keyPartList = new SqlNodeList(keyParts, pos);
}
sqlIndexOption = SqlIndexOption() {
return new SqlTableConstraint(getPos(),name,uniqueSpec,indexNmae,keyPartList,sqlIndexType,referenceDefinition,sqlIndexOption);
}
}


SqlNode TableForeignConstraint(SqlIdentifier name):
{
SqlIdentifier indexNmae = null;
SqlLiteral uniqueSpec = null;
SqlNodeList keyPartList = SqlNodeList.EMPTY ;
SqlNode referenceDefinition = null;
}
{
<KEY>{uniqueSpec = SqlConstraintSpec.FOREIGN_KEY.symbol(getPos());}
[indexNmae = SimpleIdentifier()]
<LPAREN> {
SqlParserPos pos = getPos();
List<SqlNode> keyParts = new ArrayList();
}
KeyPart(keyParts)
(
<COMMA> KeyPart(keyParts)
)*
<RPAREN>
{
pos = pos.plus(getPos());
keyPartList = new SqlNodeList(keyParts, pos);
}
referenceDefinition = ReferenceDefinition()
{
return new SqlTableConstraint(getPos(),name,uniqueSpec,indexNmae,keyPartList,null,referenceDefinition,null);
}
}



SqlNode ReferenceDefinition():
{
SqlIdentifier tableName = null;
SqlNodeList keyPartList = SqlNodeList.EMPTY ;
SqlLiteral match = null;
}
{
<REFERENCES> tableName = SimpleIdentifier()

<LPAREN> {
SqlParserPos pos = getPos();
List<SqlNode> keyParts = new ArrayList();
SqlLiteral referenceSituation = null;
SqlLiteral referenceOption = null;
}
KeyPart(keyParts)
(
<COMMA> KeyPart(keyParts)
)*
<RPAREN>
{
pos = pos.plus(getPos());
keyPartList = new SqlNodeList(keyParts, pos);
}
[
<MATCH>

<FULL>{match = MatchType.MATCH_FULL.symbol(getPos());}
|
<PARTIAL>{match = MatchType.MATCH_PARTIAL.symbol(getPos());}
|
<SIMPLE>{match = MatchType.MATCH_SIMPLE.symbol(getPos());}

]

[<ON>

(
<DELETE> {referenceSituation = ReferenceSituationEnum.ON_DELETE.symbol(getPos());} referenceOption = ReferenceOption()
|
<UPDATE>{referenceSituation = ReferenceSituationEnum.ON_UPDATE.symbol(getPos());} referenceOption = ReferenceOption()
)

]
{
return new ReferenceDefinition(getPos(),tableName,keyPartList,match,referenceSituation,referenceOption);
}
}


SqlLiteral ReferenceOption():
{
SqlLiteral referenceOption = null;
}
{

(
<RESTRICT>{ referenceOption = ReferenceOptionEnums.RESTRICT.symbol(getPos());}
|
<CASCADE>{ referenceOption = ReferenceOptionEnums.CASCADE.symbol(getPos());}
|
(
<SET>
(
<NULL> { referenceOption = ReferenceOptionEnums.SET_NULL.symbol(getPos());}
|
<DEFAULT_>{ referenceOption = ReferenceOptionEnums.SET_DEFAULT.symbol(getPos());}
)
)
|
<NO><ACTION>{ referenceOption = ReferenceOptionEnums.NO_ACTION.symbol(getPos());}

)
{
return  referenceOption;
}
}


SqlNodeList TableProperty():
{
final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
(
AttributeDef(list)
)*

{
return new SqlNodeList(list, getPos());
}
}

SqlNodeList AlterTableProperty():
{
final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
AttributeDef(list)
(
<COMMA>
AttributeDef(list)
)*

{
return new SqlNodeList(list, getPos());
}
}


void AttributeDef(List<SqlNode> list):
{
SqlLiteral key = null;
SqlNode value = null;
}
{
LOOKAHEAD(2)
(<AUTOEXTEND_SIZE>{key = MysqlTableProperty.AUTOEXTEND_SIZE.symbol(getPos()); }  [<EQ>] value = ParseStringWithStartNumber()){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<AUTO_INCREMENT>{key = MysqlTableProperty.AUTO_INCREMENT.symbol(getPos()); }  [<EQ>] value = NumericLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<AVG_ROW_LENGTH>{key = MysqlTableProperty.AVG_ROW_LENGTH.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
([<DEFAULT_>](<CHARACTER> | <CHARSET>) [<SET>]{key = MysqlTableProperty.CHARACTER_SET.symbol(getPos()); }  [<EQ>] value = SqlStringLiteralBySimpleIdentifier()
                                                    {if(key != null && value != null){
                                                    list.add( new SqlTableOption(key, value,getPos()));}
                                                    }
                                                    [<COLLATE> [<EQ>]{
                                                            SqlLiteral key1 = MysqlTableProperty.COLLATE.symbol(getPos());
                                                            SqlNode  value1 = SqlStringLiteralBySimpleIdentifier();
                                                            list.add( new SqlTableOption(key1, value1, getPos()));
                                                            }
                                                            ]
    )
|
(<CHECKSUM>{key = MysqlTableProperty.CHECKSUM.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
([<DEFAULT_>]<COLLATE>{key = MysqlTableProperty.COLLATE.symbol(getPos()); }  [<EQ>] value = SqlStringLiteralBySimpleIdentifier() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<COMMENT>{key = MysqlTableProperty.COMMENT.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<COMPRESSION>{key = MysqlTableProperty.COMPRESSION.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<CONNECTION>{key = MysqlTableProperty.CONNECTION.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<DATA><DIRECTORY>{key = MysqlTableProperty.DATA_DIRECTORY.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<INDEX><DIRECTORY>{key = MysqlTableProperty.INDEX_DIRECTORY.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<DELAY_KEY_WRITE>{key = MysqlTableProperty.DELAY_KEY_WRITE.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<ENCRYPTION>{key = MysqlTableProperty.ENCRYPTION.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<ENGINE>{key = MysqlTableProperty.ENGINE.symbol(getPos()); }  [<EQ>] value = SimpleIdentifier() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<ENGINE_ATTRIBUTE>{key = MysqlTableProperty.ENGINE_ATTRIBUTE.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<INSERT_METHOD>{key = MysqlTableProperty.INSERT_METHOD.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<KEY_BLOCK_SIZE>{key = MysqlTableProperty.KEY_BLOCK_SIZE.symbol(getPos()); }  [<EQ>] value = NumericLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<MAX_ROWS>{key = MysqlTableProperty.MAX_ROWS.symbol(getPos()); }  [<EQ>] value = UnsignedNumericLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<MIN_ROWS>{key = MysqlTableProperty.MIN_ROWS.symbol(getPos()); }  [<EQ>] value = UnsignedNumericLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<PACK_KEYS>{key = MysqlTableProperty.PACK_KEYS.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<PASSWORD>{key = MysqlTableProperty.PASSWORD.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<ROW_FORMAT>{key = MysqlTableProperty.ROW_FORMAT.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<SECONDARY_ENGINE_ATTRIBUTE>{key = MysqlTableProperty.SECONDARY_ENGINE_ATTRIBUTE.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<STATS_AUTO_RECALC>{key = MysqlTableProperty.STATS_AUTO_RECALC.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<STATS_PERSISTENT>{key = MysqlTableProperty.STATS_PERSISTENT.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<STATS_SAMPLE_PAGES>{key = MysqlTableProperty.STATS_SAMPLE_PAGES.symbol(getPos()); }  [<EQ>] value = StringLiteral() ){if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
|
(<TABLESPACE>{key = MysqlTableProperty.TABLESPACE.symbol(getPos()); }  [<EQ>] value =  SqlTableSpace() ){if(key != null && value != null){
list.add( new MysqlSqlTableOption(key, value,getPos()));}
}
|
(<UNION>{key = MysqlTableProperty.UNION.symbol(getPos()); }  [<EQ>] value = CollectionName() ){
if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
}

SqlNode SqlTableSpace():
{ SqlIdentifier tableName = null;
SqlNode storageType = null;
}
{tableName =  SimpleIdentifier()
[<STORAGE> [
<DISK>{storageType = StorageTypeEnum.DISK.symbol(getPos());}
|
<MEMORY>{storageType = StorageTypeEnum.MEMORY.symbol(getPos());}
]

]
{
return new SqlTableSpace(getPos(),tableName,storageType);
}
}



SqlNodeList CollectionName():
{
final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
CollectionNameDef(list)
(
<COMMA>  CollectionNameDef(list)
)*

{
return new SqlNodeList(list, getPos());
}
}

void CollectionNameDef(List<SqlNode> list):
{SqlNode sqlNode = null;}
{
sqlNode =StringLiteral(){
list.add(sqlNode);
}
}

// 创建函数
SqlAlterTable SqlAlterTable() :
{
SqlIdentifier tableIdentifier = null;
SqlAlterTableOperator sqlAlterTableOperator;
List<SqlAlterTableOperator> sqlAlterTableOperatorList = new ArrayList<SqlAlterTableOperator>();
}
{
<ALTER> <TABLE>
tableIdentifier =  CompoundIdentifier()
        sqlAlterTableOperator = SqlAlterOperator(tableIdentifier){
        sqlAlterTableOperatorList.add(sqlAlterTableOperator);
        }
        (<COMMA>  sqlAlterTableOperator = SqlAlterOperator(tableIdentifier){
            sqlAlterTableOperatorList.add(sqlAlterTableOperator);
            }
            )* {
 return new SqlAlterTable(getPos(),tableIdentifier ,new SqlNodeList(sqlAlterTableOperatorList, getPos()));
}
}

SqlAlterTableOperator SqlAlterOperator(SqlIdentifier tableIdentifier):
{
SqlAlterTableOperator sqlAlterTableOperator;
SqlParserPos startPos;
SqlNodeList propertyList = SqlNodeList.EMPTY;
}
{
<ADD> sqlAlterTableOperator = AlterTableAdd(tableIdentifier){
        return sqlAlterTableOperator;
}
|
<DROP> sqlAlterTableOperator = SqlAlterTableDrop(tableIdentifier){
    return sqlAlterTableOperator;
}
|
<ALTER> sqlAlterTableOperator = SqlAlterTableAlter(tableIdentifier){
    return sqlAlterTableOperator;
}
|
<ALGORITHM>sqlAlterTableOperator = SqlAlterTableAlgorithm(tableIdentifier){
    return sqlAlterTableOperator;
}
|
<CHANGE>sqlAlterTableOperator = SqlAlterTableChangeColumn(tableIdentifier){
    return sqlAlterTableOperator;
}

|
<CONVERT><TO>sqlAlterTableOperator = SqlAlterTableConvertCharacter(tableIdentifier){
        return sqlAlterTableOperator;
}
|
<DISABLE> <KEYS> {
sqlAlterTableOperator = new SqlAlterTableAbleKeys(getPos(),tableIdentifier,false);
        return sqlAlterTableOperator;
}
|
<ENABLE> <KEYS> {
sqlAlterTableOperator = new SqlAlterTableAbleKeys(getPos(),tableIdentifier,true);
        return sqlAlterTableOperator;
}
|
<DISCARD> <TABLESPACE> {
SqlLiteral tableSpacheOperator = AlterTableTableSpaceEnum.DISCARD.symbol(getPos());
sqlAlterTableOperator = new SqlAlterTableSpace(getPos(),tableIdentifier,tableSpacheOperator);
        return sqlAlterTableOperator;
}
|
<IMPORT> <TABLESPACE> {
SqlLiteral tableSpacheOperator1 = AlterTableTableSpaceEnum.IMPORT.symbol(getPos());
sqlAlterTableOperator = new SqlAlterTableSpace(getPos(),tableIdentifier,tableSpacheOperator1);
        return sqlAlterTableOperator;
}
|
<FORCE>{
sqlAlterTableOperator = new SqlAlterTableForce(getPos(),tableIdentifier);
    return sqlAlterTableOperator;
}
|
<LOCK> {
SqlLiteral lockType = null;
}(
<DEFAULT_>{
lockType = AlterTableLockTypeEnum.DEFAULT.symbol(getPos());
}
|
<NONE>{
lockType = AlterTableLockTypeEnum.NONE.symbol(getPos());
}
|
<SHARED>{
lockType = AlterTableLockTypeEnum.SHARED.symbol(getPos());
}
|
<EXCLUSIVE>{
lockType = AlterTableLockTypeEnum.EXCLUSIVE.symbol(getPos());
}
){
sqlAlterTableOperator = new SqlAlterTableLock(getPos(), tableIdentifier, lockType);
    return sqlAlterTableOperator;
}

|
<MODIFY>sqlAlterTableOperator = SqlAlterTableModifyColumn(tableIdentifier){
    return sqlAlterTableOperator;
}
|
<COMMENT> {
SqlNode comment = StringLiteral();
sqlAlterTableOperator = new SqlAlterTableComment(getPos(), tableIdentifier, comment);
return sqlAlterTableOperator;
}
|
<ORDER><BY>sqlAlterTableOperator = SqlAlterTableOrderBy(tableIdentifier){
        return sqlAlterTableOperator;
}
|
<RENAME>{
SqlLiteral renameType = null;
SqlIdentifier oldName = null;
SqlIdentifier newName = null;
}(
(
<COLUMN>{
renameType = AlterTableRenameTypeEnum.COLUMN.symbol(getPos());
oldName = SimpleIdentifier();
}
<TO>{
newName = SimpleIdentifier();
sqlAlterTableOperator = new SqlAlterTableRename(getPos(),tableIdentifier,oldName,newName,renameType);
    return sqlAlterTableOperator;
}
)
|
(
<INDEX>{
renameType = AlterTableRenameTypeEnum.INDEX.symbol(getPos());
oldName = SimpleIdentifier();
}  <TO>{
newName = SimpleIdentifier();
sqlAlterTableOperator = new SqlAlterTableRename(getPos(),tableIdentifier,oldName,newName,renameType);
        return sqlAlterTableOperator;
}
)
|
(
<KEY>{
renameType = AlterTableRenameTypeEnum.KEY.symbol(getPos());
oldName = SimpleIdentifier();
}
<TO>{
newName = SimpleIdentifier();
sqlAlterTableOperator = new SqlAlterTableRename(getPos(),tableIdentifier,oldName,newName,renameType);
    return sqlAlterTableOperator;
}
)
|
(
[<TO>|<AS>]{
renameType = AlterTableRenameTypeEnum.TABLE.symbol(getPos());
newName = SimpleIdentifier();
sqlAlterTableOperator = new SqlAlterTableRename(getPos(),tableIdentifier,oldName,newName,renameType);
            return sqlAlterTableOperator;
}
)
)
|
<WITH><VALIDATION> {
sqlAlterTableOperator =  new SqlAlterTableValidation(getPos(), tableIdentifier,SqlStringLiteral("WITH", getPos()));
        return sqlAlterTableOperator;
}
|
<WITHOUT><VALIDATION>{
sqlAlterTableOperator = new SqlAlterTableValidation(getPos(), tableIdentifier,SqlStringLiteral("WITHOUT", getPos()));
        return sqlAlterTableOperator;
}
|
(
propertyList = AlterTableProperty(){
sqlAlterTableOperator = new SqlAlterTableOptions(getPos(),tableIdentifier,propertyList);
        return sqlAlterTableOperator;
}
)
}

SqlAlterTableOperator AlterTableAdd(SqlIdentifier name):
{
SqlLiteral order =null;
SqlIdentifier coordinateColumn = null;
SqlAlterTableOperator sqlAlterTableOperator = null;
}
{
(
      LOOKAHEAD(3)
      sqlAlterTableOperator = SqlAlterTableAddIndexOrKey(name)
        |
      LOOKAHEAD(3)
      sqlAlterTableOperator = SqlAlterTableAddConstraint(name)
        |
      LOOKAHEAD(3)
      [<COLUMN>]  sqlAlterTableOperator = SqlAlterTableAddColumn(name)

){
return sqlAlterTableOperator;
}
}

SqlAlterTableOperator SqlAlterTableAddColumn(SqlIdentifier name):
{
SqlAlterTableAddColumn SqlAlterTableAddColumn = null;
SqlNodeList columns = SqlNodeList.EMPTY;
TableCreationContext ctx = new TableCreationContext();
SqlIdentifier coordinateColumn = null;
SqlLiteral order = null;
SqlParserPos pos = getPos();
}
{
(

     (
        <LPAREN>{
            pos = getPos();
            }
            TableColumn(ctx)
            (
            <COMMA> TableColumn(ctx)
                )*{
                if(CollectionUtils.isNotEmpty(ctx.columnList)){
                columns =new SqlNodeList(ctx.columnList, pos);
                }
        }<RPAREN>
                )
|
        TableColumn(ctx){
            if(CollectionUtils.isNotEmpty(ctx.columnList)){
            columns =new SqlNodeList(ctx.columnList, pos);
            }
            }
)
[
<FIRST>{  order = OrderEnums.FIRST.symbol(getPos()); coordinateColumn = SimpleIdentifier(); }
|
<AFTER>{  order = OrderEnums.AFTER.symbol(getPos()); coordinateColumn = SimpleIdentifier();}
]{
return new SqlAlterTableAddColumn(getPos(),name,columns,order,coordinateColumn);
}
}


SqlAlterTableOperator SqlAlterTableAddIndexOrKey(SqlIdentifier name):
{
SqlAlterTable sqlAlterIndexOrKey = null;
TableCreationContext ctx = new TableCreationContext();
}
{
TableIndex(ctx) {
if(CollectionUtils.isNotEmpty(ctx.indexList)){
return new SqlAlterTableAddIndex(getPos(),name, ctx.indexList.get(0)) ;
}
}
}

SqlAlterTableOperator SqlAlterTableAddConstraint(SqlIdentifier name):
{
SqlAlterTable sqlAlterIndexOrKey = null;
TableCreationContext ctx = new TableCreationContext();
}
{
TableConstraint(ctx){
if(CollectionUtils.isNotEmpty(ctx.constraintList)){
return new SqlAlterTableAddConstraint(getPos(),name,(SqlTableConstraint)ctx.constraintList.get(0)) ;
}
if(CollectionUtils.isNotEmpty(ctx.checkConstraintList)){
return new SqlAlterTableAddConstraint(getPos(),name,ctx.checkConstraintList.get(0)) ;
}
}
}

SqlAlterTableOperator SqlAlterTableChangeColumn (SqlIdentifier name):
    {
    SqlIdentifier oldColumnName;
     SqlNode newColum = null;
     TableCreationContext ctx = new TableCreationContext();
     SqlIdentifier coordinateColumn = null;
     SqlLiteral order = null;
    }
    {
    [<COLUMN>] oldColumnName = SimpleIdentifier()
        TableColumn(ctx){
        if(CollectionUtils.isNotEmpty(ctx.columnList)){
        newColum = ctx.columnList.get(0);
        }
        }
        [
        <FIRST>{  order = OrderEnums.FIRST.symbol(getPos()); coordinateColumn = SimpleIdentifier(); }
            |
        <AFTER>{  order = OrderEnums.AFTER.symbol(getPos()); coordinateColumn = SimpleIdentifier();}
       ]{
            return new SqlAlterTableChangeColumn(getPos(),name,oldColumnName,newColum,order,coordinateColumn);
          }
    }

SqlAlterTableOperator SqlAlterTableModifyColumn (SqlIdentifier name):
    {
    SqlNode newColum = null;
    TableCreationContext ctx = new TableCreationContext();
    SqlIdentifier coordinateColumn = null;
    SqlLiteral order = null;
    }
    {
    [<COLUMN>]
        TableColumn(ctx){
        if(CollectionUtils.isNotEmpty(ctx.columnList)){
        newColum = ctx.columnList.get(0);
        }
   }
        [
        <FIRST>{  order = OrderEnums.FIRST.symbol(getPos()); coordinateColumn = SimpleIdentifier(); }
            |
            <AFTER>{  order = OrderEnums.AFTER.symbol(getPos()); coordinateColumn = SimpleIdentifier();}
        ]{
                return new SqlAlterTableChangeColumn(getPos(),name,null,newColum,order,coordinateColumn);
            }
        }


SqlAlterTableOperator SqlAlterTableOrderBy(SqlIdentifier name):
    {
    List<SqlNode>columns = new ArrayList();
    }
    {
        KeyPart(columns)
        (
        <COMMA> KeyPart(columns)
            )* {
            return new SqlAlterTableOrderBy(getPos(),name, new SqlNodeList(columns, getPos()));
            }
    }


SqlAlterTableOperator SqlAlterTableConvertCharacter (SqlIdentifier name):
{
            SqlNode character = null;
            SqlNode collate = null;
}
{
         (
            [<CHARACTER><SET> [<EQ>]
                        character =  SqlStringLiteralBySimpleIdentifier()
            ]
            [
            <COLLATE> [<EQ>]
                    collate = SqlStringLiteralBySimpleIdentifier()
            ]
            ){
                 return new SqlAlterTableCharacterAndCollate(getPos(),name,character,collate);
             }
}


SqlAlterTableOperator SqlAlterTableAlter(SqlIdentifier name):
{
SqlAlterTableOperator sqlAlterTableOperator = null;
}
{
    sqlAlterTableOperator = SqlAlterTableConstraint(name){
    return sqlAlterTableOperator;
    }
    |
    sqlAlterTableOperator = SqlAlterTableIndex(name){
    return sqlAlterTableOperator;
    }
    |
    sqlAlterTableOperator = SqlAlterTableColumn(name){
    return sqlAlterTableOperator;
    }
}

SqlAlterTableOperator SqlAlterTableConstraint(SqlIdentifier name):
{       SqlLiteral altertType = null;
        SqlIdentifier symbol = null;
        SqlLiteral  enforcement = null;
}
{
      (  <CONSTRAINT> {altertType = AlterTableTargetTypeEnum.CONSTRAINT.symbol(getPos()) ;}
            |
            <CHECK> {altertType = AlterTableTargetTypeEnum.CHECK.symbol(getPos()) ;}
      )
        symbol = SimpleIdentifier()
        enforcement = ConstraintEnforcement() {
        return new SqlAlterTableConstraint(getPos(),name,altertType,symbol,enforcement);
        }
}


SqlAlterTableOperator SqlAlterTableIndex(SqlIdentifier name):
{
SqlIdentifier symbol = null;
SqlLiteral  visiable = null;
}
{
<INDEX>
symbol = SimpleIdentifier()
(
<VISIBLE> { visiable = SqlMysqlConstraintEnable.VISIBLE.symbol(getPos()); }
|
<INVISIBLE> { visiable = SqlMysqlConstraintEnable.INVISIBLE.symbol(getPos()); }
){
return new SqlAlterTableIndex(getPos(),name,symbol,visiable);
}
}

SqlAlterTableOperator SqlAlterTableColumn(SqlIdentifier name):
    {
    SqlIdentifier symbol = null;
    SqlLiteral  operator = null;
    SqlNode  value = null;
    }
    {
    [<COLUMN>]
        symbol = SimpleIdentifier()
       (
        <DROP> {
            return SqlAlterTableAlterColumnDrop(name, symbol);
            }
            |
            <SET>{
                return SqlAlterTableAlterColumnSet(name, symbol);
                }
        )
  }

SqlAlterTableOperator SqlAlterTableAlterColumnDrop(SqlIdentifier name,SqlIdentifier symbol):
{

}
{
<DEFAULT_>{
  SqlLiteral  operator = AlterTableColumnOperatorTypeEnum.DROP_DEFAULT.symbol(getPos());
  return new SqlAlterTableAlterColumn(getPos(),name,symbol,operator,null);
  }

}


SqlAlterTableOperator SqlAlterTableAlterColumnSet(SqlIdentifier name,SqlIdentifier symbol):
    {
    SqlLiteral  operator = null;
    SqlNode  value = null;
    }
    {
    <DEFAULT_> {
        operator = AlterTableColumnOperatorTypeEnum.SET_DEFAULT.symbol(getPos());
        value  = Expression(ExprContext.ACCEPT_SUB_QUERY);
        return new SqlAlterTableAlterColumn(getPos(),name,symbol,operator,value);
        }
        |
        <VISIBLE> {
            operator = AlterTableColumnOperatorTypeEnum.SET.symbol(getPos());
            value = SqlMysqlConstraintEnable.VISIBLE.symbol(getPos());
            return new SqlAlterTableAlterColumn(getPos(),name,symbol,operator,value);
            }

            |
            <INVISIBLE> {
                operator = AlterTableColumnOperatorTypeEnum.SET.symbol(getPos());
                value = SqlMysqlConstraintEnable.INVISIBLE.symbol(getPos());
                return new SqlAlterTableAlterColumn(getPos(),name,symbol,operator,value);
                }
                }



                SqlAlterTableOperator SqlAlterTableAlgorithm(SqlIdentifier name):
                {
                SqlAlterTableOperator sqlAlterTableOperator = null;
                SqlLiteral algorithmType = null;
                }
                {
              [<EQ>]
                  (
                    <DEFAULT_>{
                        algorithmType = AlterTableAlgorithmType. DEFAULT.symbol(getPos());
                        return new SqlAlterTableAlgorithm(getPos(),name,algorithmType);
                        }
                        |
                        algorithmType = SqlStringLiteralBySimpleIdentifier(){
                        return new SqlAlterTableAlgorithm(getPos(),name,algorithmType);
                        }
                    )

                }



SqlAlterTableOperator SqlAlterTableDrop(SqlIdentifier name):
{
SqlLiteral dropType = null;
SqlIdentifier dropName = null;
}
{
(
<CHECK> {
dropType =AlterTableTargetTypeEnum.CHECK.symbol(getPos()) ;
}
|
<INDEX>{
dropType =AlterTableTargetTypeEnum.INDEX.symbol(getPos()) ;
}
|
<KEY>{
dropType =AlterTableTargetTypeEnum.KEY.symbol(getPos());
}
|
<CONSTRAINT> {
dropType =AlterTableTargetTypeEnum.CONSTRAINT.symbol(getPos());
}
){
dropName = SimpleIdentifier();
return new SqlAlterTableDrop(getPos(),name,dropType,dropName);
}

|
(<PRIMARY><KEY>{
dropType =AlterTableTargetTypeEnum.PRIMARY_kEY.symbol(getPos());
}){
return new SqlAlterTableDrop(getPos(),name,dropType,dropName);
}
|
(<FOREIGN><KEY>{
dropType =AlterTableTargetTypeEnum.FOREIGN_kEY.symbol(getPos());
}){
dropName = SimpleIdentifier();
return new SqlAlterTableDrop(getPos(),name,dropType,dropName);
}
|
(
[<COLUMN>]
{
dropType =AlterTableTargetTypeEnum.COLUMN.symbol(getPos());
}
){
dropName = SimpleIdentifier();
return new SqlAlterTableDrop(getPos(),name,dropType,dropName);
}
}


SqlNode SqlIndexOption():
{
     SqlNode keyBlockSize = null;
     SqlNode withParse = null;
     SqlNode comment = null;
     SqlLiteral visiable = null;
     SqlNode engineAttribute = null;
     SqlNode secondEngineAttribute = null;
    SqlNode indexType = null;
}{
(
<KEY_BLOCK_SIZE> [<EQ>] {keyBlockSize = NumericLiteral();}
|
indexType = SqlIndexType()
|
<WITH> <PARSER> {withParse = SqlStringLiteralBySimpleIdentifier(); }
|
<COMMENT> {comment = StringLiteral(); }
|
<VISIBLE> {visiable = SqlMysqlConstraintEnable.VISIBLE.symbol(getPos()); }
|
<INVISIBLE> { visiable = SqlMysqlConstraintEnable.INVISIBLE.symbol(getPos()); }
|
<ENGINE_ATTRIBUTE> [<EQ>] {engineAttribute = StringLiteral();}
|
<SECONDARY_ENGINE_ATTRIBUTE> [<EQ>]{secondEngineAttribute = StringLiteral();}
)*{
return new SqlIndexOption(getPos(),keyBlockSize,indexType,withParse,comment,visiable,engineAttribute,secondEngineAttribute);}
}

SqlNode SqlIndexType():
{
SqlIndexType sqlIndexType = null;
}{
[<USING>]
(
<BTREE> {sqlIndexType = new SqlIndexType(getPos(),SqlLiteral.createSymbol(IndexStorageType.BTREE, getPos())); }
|
<HASH> {sqlIndexType = new SqlIndexType(getPos(),SqlLiteral.createSymbol(IndexStorageType.HASH, getPos())); }
)
{
return sqlIndexType;
}
}


SqlNode TruncateTable():
{
SqlIdentifier name = null;
}
{
<TRUNCATE> <TABLE> name = CompoundIdentifier() {
    return new SqlTruncateTable(getPos(), name);
    }
}

SqlNode RenameTable():
{
List<SqlNode>renameTables = new ArrayList();
}
{
<RENAME><TABLE>RenameSingleton(renameTables)
    (
    <COMMA> RenameSingleton(renameTables)
    )*{
    return new SqlRenameTable(getPos(),new SqlNodeList(renameTables,getPos()));
    }

}

void RenameSingleton(List<SqlNode> renameTables):
{
SqlIdentifier oldName = null;
SqlIdentifier newName = null;
}{
oldName = CompoundIdentifier()
<TO>
newName = CompoundIdentifier(){
        renameTables.add(new SqlRenameTableSingleton(getPos(),oldName,newName));
        }
}


    SqlDrop SqlDropView(Span s, boolean replace) :
    {
     boolean ifExists;
     ArrayList<SqlNode> list = new ArrayList();
    Boolean cascade = null ;
     SqlIdentifier name ;
    }
    {
    <VIEW> ifExists = IfExistsOpt()
        name = CompoundIdentifier(){
        list.add(name);
        }
        (
        <COMMA> name = CompoundIdentifier(){
            list.add(name);
          }
         )*

            [
            <RESTRICT> { cascade = false; }
                |
           <CASCADE>  { cascade = true; }
          ] {
            return new SqlDropView(getPos(),ifExists,new SqlNodeList(list,getPos()),cascade);
            }
    }


   SqlDrop SqlDropTriggerOrServerOrFunctionOrEventOrDataBase(Span s, boolean replace) :
   {
       boolean ifExists;
       Boolean cascade = null ;
       SqlIdentifier name ;
       }
       {
       <TRIGGER> ifExists = IfExistsOpt()
           name = CompoundIdentifier(){
           return new SqlDropTrigger(getPos(),ifExists,name);
           }
           |
           <SERVER> ifExists = IfExistsOpt()
               name = CompoundIdentifier(){
               return new SqlDropServer(getPos(),ifExists,name);
               }
               |
               <PROCEDURE>ifExists = IfExistsOpt()
                   name = CompoundIdentifier(){
                   return new SqlDropProcedure(getPos(),ifExists,name);
                   }
              |
               <FUNCTION> ifExists = IfExistsOpt()
                   name = CompoundIdentifier(){
                   return new SqlDropFunction(getPos(),ifExists,name);
                   }
               |
               <EVENT> ifExists = IfExistsOpt()
                   name = CompoundIdentifier(){
                   return new SqlDropEvent(getPos(),ifExists,name);
                   }
                |
               <DATABASE> ifExists = IfExistsOpt()
                   name = CompoundIdentifier(){
                   return new SqlDropDataBase(getPos(),ifExists,name);
                   }
                 |
               <SCHEMA> ifExists = IfExistsOpt()
                   name = CompoundIdentifier(){
                   return new SqlDropSchema(getPos(),ifExists,name);
                   }
        }


       SqlDrop SqlDropTableSpace(Span s, boolean replace) :
       {
       boolean isUndo = false;
       SqlIdentifier name ;
       SqlLiteral engineName = null ;
       }
       {
           [<UNDO>{isUndo = true;}]
           <TABLESPACE>
           name = CompoundIdentifier()
             [<ENGINE>[<EQ>]{engineName = SqlStringLiteralBySimpleIdentifier();}]{
               return new SqlDropTableSpace(getPos(),name,isUndo,engineName);
               }
           }

           SqlDrop SqlDropLogfileGroup(Span s, boolean replace) :
           {
           SqlIdentifier name ;
           SqlLiteral engineName = null ;
           }
           {
               <LOGFILE><GROUP>
                   name = CompoundIdentifier()
                   [<ENGINE>[<EQ>]{engineName = SqlStringLiteralBySimpleIdentifier();}]{
                           return new SqlDropLogfileGroup(getPos(),name,engineName);
                           }
          }

       SqlDrop SqlDropTable(Span s, boolean replace) :
       {
           boolean isTemporary = false;
           boolean ifExists;
           ArrayList<SqlNode> list = new ArrayList();
           Boolean cascade = null ;
           SqlIdentifier name ;
        }
        {
         [<TEMPORARY>{isTemporary = true;}]
           <TABLE> ifExists = IfExistsOpt()
               name = CompoundIdentifier(){
               list.add(name);
               }
               (
               <COMMA> name = CompoundIdentifier(){
                   list.add(name);
                   }
              )*

               [
               <RESTRICT> { cascade = false; }
                |
                <CASCADE>  { cascade = true; }
                ] {
                       return new SqlDropTable(getPos(),isTemporary, ifExists, new SqlNodeList(list,getPos()),cascade);
                  }
           }


        SqlDrop SqlDropSpatialSystem(Span s, boolean replace) :
        {
        SqlNode srid = null;
         boolean ifExists;
        }
        {
            <SPATIAL><REFERENCE><SYSTEM>
            ifExists = IfExistsOpt()
            srid = UnsignedNumericLiteral(){
            return new SqlDropSpatialSystem(getPos(),ifExists,srid);
            }
        }

      SqlDrop SqlDropIndex(Span s, boolean replace) :
        {
                        SqlIdentifier indexName;
                        SqlIdentifier tableName;
                        SqlLiteral algorithmOption = null;
                        SqlLiteral lockOption = null;

        }{
        <INDEX>
            indexName = SimpleIdentifier()
            <ON>
            tableName = CompoundIdentifier()
     (
        (
        <ALGORITHM> [<EQ>]
                (
                <DEFAULT_>{ algorithmOption = AlterTableAlgorithmType.DEFAULT.symbol(getPos()); }
                    |
                <INPLACE>{ algorithmOption = AlterTableAlgorithmType.INPLACE.symbol(getPos()); }
                    |
               <COPY>{ algorithmOption = AlterTableAlgorithmType.COPY.symbol(getPos()); }
                )
        )
       |
       <LOCK> [<EQ>] (
           <DEFAULT_>{
               lockOption = AlterTableLockTypeEnum.DEFAULT.symbol(getPos());
               }
           |
            <NONE>{
                lockOption = AlterTableLockTypeEnum.NONE.symbol(getPos());
                   }
          |
           <SHARED>{
               lockOption = AlterTableLockTypeEnum.SHARED.symbol(getPos());
               }
          |
         <EXCLUSIVE>{
             lockOption = AlterTableLockTypeEnum.EXCLUSIVE.symbol(getPos());
                   }
           )

     )*{
           return new SqlDropIndex(getPos(), new SqlIndex(getPos(), indexName, null, null, null, null, algorithmOption, lockOption), tableName);
           }
        }


SqlAlterDataBase SqlAlterDataBase():
{
             SqlParserPos startPos;
             SqlIdentifier databaseName;
             SqlNodeList propertyList;
             boolean isDataBase = false;
}
{
 <ALTER> ( <DATABASE> {isDataBase = true;} | <SCHEMA> {isDataBase = false;}) { startPos = getPos(); }
         databaseName = CompoundIdentifier()
         propertyList = DataBaseProperty(){
         return new SqlAlterDataBase(getPos(),isDataBase,databaseName,propertyList);
         }
}


SqlNodeList DataBaseProperty():
{
final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
(
DataBaseAttributeDef(list)
)*

{
return new SqlNodeList(list, getPos());
}
}


void DataBaseAttributeDef(List<SqlNode> list):
{
SqlLiteral key = null;
SqlNode value = null;
}
{
[<DEFAULT_>]
(
<COLLATE>{key = MysqlDataBaseProperty.COLLATE.symbol(getPos()); }  [<EQ>] value = SqlStringLiteralBySimpleIdentifier(){if(key != null && value != null){
list.add( new SqlDataBaseOption(getPos(), key, value));}
        }
|
<CHARACTER> <SET>{key = MysqlDataBaseProperty.CHARACTER_SET.symbol(getPos()); }  [<EQ>] value = SqlStringLiteralBySimpleIdentifier()
{if(key != null && value != null){
list.add( new SqlDataBaseOption(getPos(), key, value));}
}
|
<ENCRYPTION>{key = MysqlDataBaseProperty.ENCRYPTION.symbol(getPos()); } [<EQ>] value = StringLiteral(){
if(key != null && value != null){
list.add( new SqlDataBaseOption(getPos(), key, value));}
}
)
|
(<READ><ONLY>{key = MysqlDataBaseProperty.READ_ONLY.symbol(getPos()); }  [<EQ>]
(
<DEFAULT_>{value = SqlLiteral.createCharString(token.image, getPos()) ; }
|
value = NumericLiteral()
)
){
if(key != null && value != null){
list.add( new SqlTableOption(key, value,getPos()));}
}
}

SqlAlterEvent SqlAlterEvent():
{
SqlIdentifier eventName;
SqlNode schedule = null;
Boolean isPreserve = null;
SqlIdentifier user = null;
SqlIdentifier newEventName = null;
SqlNode doLiteral = null;
SqlNode comment = null;
SqlLiteral enable = null;
}
{
<ALTER> [<DEFINER>{user = SimpleIdentifier(); }]  <EVENT> eventName = CompoundIdentifier()
(
    (
    <ON> (
        <SCHEDULE>{schedule = AtomicRowExpression();}
            |
            <COMPLETION> (
                <NOT><PRESERVE>{isPreserve = false;}
                        |
                        <COMPLETION> <PRESERVE>{isPreserve = true;}
                                )
                                )

                                )
                       |

    (<RENAME><TO>{newEventName = CompoundIdentifier();})
     |
    (<COMMENT>{comment = StringLiteral();})
      |
    (<DO>{doLiteral = AtomicRowExpression();})
    |

    (
    <ENABLE>{enable = SqlStringLiteral("ENABLE", getPos()); }
        |
        <DISABLE> (
            {enable = SqlStringLiteral("DISABLE", getPos()); }
            |
            <ON><SLAVE>{enable = SqlStringLiteral("DISABLE ON SLAVE", getPos()); }
                   )
    )
    )*{
return new SqlAlterEvent(getPos(),eventName,schedule,isPreserve,user,newEventName,doLiteral,comment,enable);
}
}


SqlAlterFunction SqlAlterFunction():
  {
  SqlIdentifier functionName;
  SqlNode sqlProcedureCharacteristic = null;
  }
  {
  <ALTER><FUNCTION>functionName = CompoundIdentifier()
          sqlProcedureCharacteristic = SqlProcedureCharacteristic(){
           return new SqlAlterFunction(getPos(),functionName,sqlProcedureCharacteristic);
          }
  }

  SqlAlterInstance SqlAlterInstance():
     {
     List<SqlLiteral>instanceActionList = new ArrayList();
     SqlLiteral instanceAction = null;
     }
     {
     <ALTER><INSTANCE>
     (
             (<ENABLE> <INNODB><REDO_LOG>{instanceAction = AlterInstanceActionEnum.ENABLE_INNODB_REDO_LOG.symbol(getPos()); instanceActionList.add(instanceAction);})
                 |
             (<DISABLE> <INNODB><REDO_LOG>{instanceAction = AlterInstanceActionEnum.DISABLE_INNODB_REDO_LOG.symbol(getPos());instanceActionList.add(instanceAction);})
                 |
             (<ROTATE>
                     ((<INNODB><MASTER><KEY>){instanceAction = AlterInstanceActionEnum.ROTATE_INNODB_MASTER_KEY.symbol(getPos());instanceActionList.add(instanceAction);}
                       |
                       (<BINLOG><MASTER><KEY>){instanceAction = AlterInstanceActionEnum.ROTATE_BINLOG_MASTER_KEY.symbol(getPos());instanceActionList.add(instanceAction);}
                     )
             )
                 |
              (<RELOAD>(
                           (<TLS>{instanceAction = AlterInstanceActionEnum.RELOAD_TLS.symbol(getPos());}
                                       [
                                     (<FOR><CHANNEL>{instanceAction = SqlStringLiteral(AlterInstanceActionEnum.RELOAD_TLS_FOR_CHANNEL.getDigest() +" "+ SimpleIdentifierValue(),getPos());})
                                             |
                                     (<NO><ROLLBACK><ON><ERROR>{instanceAction = AlterInstanceActionEnum.RELOAD_TLS_NO_ROLLBACK_ON_ERROR.symbol(getPos());})
                                       ]
                           ){
                             instanceActionList.add(instanceAction);
                             }
                                |
                           (<KEYRING>{instanceAction = AlterInstanceActionEnum.RELOAD_KEYRING.symbol(getPos());instanceActionList.add(instanceAction);})
                       )
              )
     )*{
       return new SqlAlterInstance(getPos(),new SqlNodeList(instanceActionList,getPos())); }
}

 SqlAlterLogFileGroup SqlAlterLogFileGroup():
     {
       SqlIdentifier logfileGroup;
       SqlNode fileNmae = null;
       SqlNode size = null;
       SqlLiteral  engineName = null;
     }
     {
         <ALTER><LOGFILE><GROUP>logfileGroup = CompoundIdentifier()
          <ADD><UNDOFILE>fileNmae = StringLiteral()
          (
            <WAIT>
                |
             (<INITIAL_SIZE> [<EQ>] {size = ParseStringWithStartNumber();})
            )*
          <ENGINE>[<EQ>] {
                  engineName = SqlStringLiteralBySimpleIdentifier();
                  return new SqlAlterLogFileGroup(getPos(), logfileGroup, fileNmae, size, engineName);
                  }
     }


  SqlAlterProcedure SqlAlterProcedure():
  {
      SqlIdentifier procName;
      SqlNode characteristic = null;
}
  {
      <ALTER><PROCEDURE>procName = CompoundIdentifier()
              characteristic = SqlProcedureCharacteristic(){
              return new SqlAlterProcedure(getPos(), procName, characteristic);
              }
  }

SqlProcedureCharacteristic SqlProcedureCharacteristic():
      {
      SqlNode comment = null;
      SqlLiteral sqlType = null;
      SqlLiteral languageSql = null;
      SqlLiteral sqlSecurity = null;
      }
      {
      (
  (<COMMENT>{comment = StringLiteral();})
      |
      (<LANGUAGE><SQL>{languageSql = SqlStringLiteral("LANGUAGE SQL", getPos());})
              |
      (<CONTAINS><SQL>{sqlType = SqlStringLiteral("CONTAINS SQL", getPos()); })
              |
      (<NO><SQL>{sqlType = SqlStringLiteral("NO SQL", getPos()); })
              |
      (<READS><SQL><DATA>{sqlType = SqlStringLiteral("READS SQL DATA", getPos()); })
              |
              (<MODIFIES><SQL><DATA>{sqlType = SqlStringLiteral("MODIFIES SQL DATA", getPos()); })
              |
      (<SQL><SECURITY>(<DEFINER>{sqlSecurity = SqlStringLiteral("DEFINER", getPos()); }
                         |
                        <INVOKER>{sqlSecurity = SqlStringLiteral("INVOKER", getPos()); }
                         )
      )
      )*{
          return new SqlProcedureCharacteristic(getPos(),comment,sqlType,languageSql,sqlSecurity);
        }
      }

                      SqlAlterServer SqlAlterServer():
                            {
                            SqlIdentifier serverName;
                            List<SqlNode> sqlNodeList = new ArrayList();
                            SqlNode sqlNode =null;
                            }
                            {
                            <ALTER><SERVER>serverName = CompoundIdentifier()
                                    <OPTIONS>
                                    <LPAREN>
                                        sqlNode = SqlServerOption(){
                                        sqlNodeList.add(sqlNode);
                                        }
                                        (
                                        <COMMA> sqlNode = SqlServerOption(){
                                            sqlNodeList.add(sqlNode);
                                            }
                                        )*
                                     <RPAREN>{
                                         return new SqlAlterServer(getPos(), serverName, new SqlNodeList(sqlNodeList, getPos()) );
                                         }
                            }

                   SqlServerOption SqlServerOption():
                     {
                                         SqlNode key = null;
                                         SqlNode value = null;
                     }
                     {
                        (
                           (<HOST> {key = ServerOptionEnum.HOST.symbol(getPos()); } value = StringLiteral())
                                             |
                           (<DATABASE> {key = ServerOptionEnum.DATABASE.symbol(getPos()); } value = StringLiteral())
                                             |
                           (<USER> {key = ServerOptionEnum.USER.symbol(getPos()); } value = StringLiteral())
                                             |
                           (<PASSWORD> {key = ServerOptionEnum.PASSWORD.symbol(getPos()); } value = StringLiteral())
                                             |
                           (<SOCKET> {key = ServerOptionEnum.SOCKET.symbol(getPos()); } value = StringLiteral())
                                             |
                           (<OWNER> {key = ServerOptionEnum.OWNER.symbol(getPos()); } value = StringLiteral())
                                             |
                           (<PORT> {key = ServerOptionEnum.PORT.symbol(getPos()); } value = UnsignedNumericLiteral())
                        ){
                           return new SqlServerOption(getPos(), key, value);
                         }
                     }
