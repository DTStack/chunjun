<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

// https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-TABLE.html#GUID-F9CE0CC3-13AE-4744-A43C-EAC7A71AAAB6
SqlCreate SqlCreateTableParse(Span s, boolean isReplace) :
{
final SqlIdentifier tableIdentify;
boolean isTemporary = false;
SqlNodeList columnList = new SqlNodeList(getPos());
SqlNodeList constraintList = new SqlNodeList(getPos());
Span span = Span.of(getPos());
}
{
[
<SHARDED> {throw new UnsupportedOperationException("unsupported SHADED table");}
|
<DUPLICATED> {throw new UnsupportedOperationException("unsupported DUPLICATED table");}
|
<IMMUTABLE> {throw new UnsupportedOperationException("unsupported IMMUTABLE/IMMUTABLE BLOCKCHAIN table");}
|
<BLOCKCHAIN> {throw new UnsupportedOperationException("unsupported BLOCKCHAIN table");}
|
(<GLOBAL>{throw new UnsupportedOperationException("only support PRIVATE TEMPORARY table now");}|<PRIVATE> <TEMPORARY>{ isTemporary = true; } )
]
<TABLE> tableIdentify = CompoundIdentifier()
[<SHARING> {throw new UnsupportedOperationException("unsupported SHARING definition");}]
[<OF> {throw new UnsupportedOperationException("only support relational table now");}]
parseRelationalTable(columnList, constraintList)
[<MEMOPTIMIZE> {throw new UnsupportedOperationException("unsupported MEMOPTIMIZE definition");}]
[<PARENT> {throw new UnsupportedOperationException("unsupported PARENT definition");}]
{return new SqlCreateTable(span.end(this), tableIdentify, isTemporary, columnList, constraintList);}
}

void parseRelationalTable(SqlNodeList columnList, SqlNodeList constraintList) :
{
}
{
[<LPAREN>parseRelationalProperties(columnList, constraintList)(<COMMA> parseRelationalProperties(columnList, constraintList))*<RPAREN>]
[<NO> <DROP> {throw new UnsupportedOperationException("unsupported immutable_table_clauses and blockchain_table_clauses");}]
[<DEFAULT_> <COLLATION> {throw new UnsupportedOperationException("unsupported COLLATION definition");}]
[<ON> <COMMIT> {throw new UnsupportedOperationException("unsupported ON COMMIT definition");}]
[parsePhysicalProperties() {throw new UnsupportedOperationException("unsupported PhysicalProperties");}]
[parseTableProperties() {throw new UnsupportedOperationException("unsupported table_properties");}]
}


void parseRelationalProperties(SqlNodeList columnList, SqlNodeList constraintList) :
{
}
{
(
// parse column and virtual column
parseColumnDefinition(columnList, constraintList)
|
parsePeriodDefinition()
|
// parse outOfLineConstraint And outOfLineRefConstraint
parseOutOfLineConstraintDefinition(constraintList)
|
parseSupplementalLoggingPropsDefinition()
)
}

void parseColumnDefinition(SqlNodeList columnList,SqlNodeList constraintList) :
{
final SqlIdentifier columnName;
SqlDataTypeSpec type = null;
SqlIdentifier columnCollationName = null;
SqlNode defaultValue = null ;
Span span = Span.of(getPos());
}
{
columnName = SimpleIdentifier()
[type = DataType() [<COLLATE> columnCollationName = SimpleIdentifier()]]
{columnList.add(new SqlGeneralColumn(span.end(this), columnName, type, defaultValue));}
[<SORT> [<VISIBLE>|<INVISIBLE>]]
[
(<DEFAULT_> [<ON> <NULL>])defaultValue = parseDefaultValueExpression()
|
parseIdentifyClause(){throw new UnsupportedOperationException("unsupported column identify clause");}
]
[<ENCRYPT> {parseEncryptionSpec(); throw new UnsupportedOperationException("unsupported encrypt column");}]
[
(parseInlineConstraintDefinition(columnName,constraintList) (parseInlineConstraintDefinition(columnName,constraintList))*)
|
parseInlineRefConstraintDefinition(){throw new UnsupportedOperationException("unsupported column inlineRefConstraint");}
]
// virtual column
[[<GENERATED> <ALWAYS>] <AS> {throw new UnsupportedOperationException("unsupported virtual column");}]
}

void parseIdentifyClause() :
{
}
{
<GENERATED>
[<ALWAYS>|(<BY> <DEFAULT_>[<ON> <NULL>])]
[<AS> <IDENTIFY>]
[<LPAREN> parseIdentifyOptions() (<COMMA> parseIdentifyOptions())* <RPAREN>]
}

void parseIdentifyOptions() :
{
}
{
(<START> <WITH>)
|
(<INCREMENT> <BY>)
|
(<MAXVALUE> IntLiteral())
|
<NOMAXVALUE>
|
(<MINVALUE> IntLiteral())
|
<NOMINVALUE>
|
<CYCLE>
|
<NOCYCLE>
|(<CACHE> IntLiteral())
|<NOCACHE>
|<ORDER>
|<NOORDER>
}


SqlNode parseDefaultValueExpression() :
{
SqlNode defaultValue = null;
}
{
defaultValue = Expression(ExprContext.ACCEPT_SUB_QUERY)
{
if(defaultValue instanceof SqlLiteral ){
SqlLiteral sqliteral = (SqlLiteral) defaultValue;
if(sqliteral.getTypeName().equals(SqlTypeName.NULL)){
defaultValue = null;
}
}
}
{return defaultValue;}
}

void parseEncryptionSpec() :
{
SqlIdentifier encryptAlgorithm = null;
SqlIdentifier password = null;
SqlIdentifier integrityAlgorithm = null;
}
{
[<USING> encryptAlgorithm = SimpleIdentifier() ]
[<IDENTIFY> <BY> password = SimpleIdentifier()]
[integrityAlgorithm = SimpleIdentifier()]
[[<NO>] <SLAT>]
}

void parseInlineConstraintDefinition(SqlIdentifier columnName,SqlNodeList constraintList) :
{
SqlIdentifier constraintName = null;
SqlNodeList columns = new SqlNodeList(getPos());
SqlLiteral constraintSpec = null;
SqlConstraintReferences references = null;
SqlNode checkCondition = null;
Span span = Span.of(getPos());
}
{
[<CONSTRAINT> constraintName = SimpleIdentifier()]
constraintSpec = parseConstraintSpec()
{
if(constraintSpec.getValue().equals(SqlConstraintSpec.CHECK)){
checkCondition = parseConstraintCheckExpression();
} else if(constraintSpec.getValue().equals(SqlConstraintSpec.REFERENCES)){
references = parseReferencesSpec();
}
columns.add(columnName);
constraintList.add(new SqlConstraint(span.end(this), constraintName, columns, constraintSpec, references, checkCondition));
}
}

SqlLiteral parseConstraintSpec() :
{
SqlConstraintSpec sqlConstraintSpec = null;
boolean nullable = true;
Span span = Span.of(getPos());
}
{
(
<UNIQUE>{sqlConstraintSpec = SqlConstraintSpec.UNIQUE;}
|(<PRIMARY> <KEY>){sqlConstraintSpec = SqlConstraintSpec.PRIMARY_KEY;}
|([<NOT> {nullable = false;}] <NULL>) {if(nullable){sqlConstraintSpec = SqlConstraintSpec.NULL;}else{sqlConstraintSpec = SqlConstraintSpec.NOT_NULL;}}
|(<CHECK> {sqlConstraintSpec = SqlConstraintSpec.CHECK;})
|(<REFERENCES> {sqlConstraintSpec = SqlConstraintSpec.REFERENCES;})
|(<FOREIGN> <KEY> {sqlConstraintSpec = SqlConstraintSpec.FOREIGN_KEY;})
)
{return sqlConstraintSpec.symbol(span.end(this));}
}

SqlConstraintReferences parseReferencesSpec() :
{
SqlIdentifier referencesTable;
SqlNodeList referencesColumns = null;
SqlLiteral referencesSituation = null;
SqlLiteral referencesOption = null;
Span span = Span.of(getPos());
Span situationSpan = null;
}
{
referencesTable = CompoundIdentifier()
[referencesColumns = ParenthesizedSimpleIdentifierList()]
{situationSpan = Span.of(getPos());}
[
<ON> <DELETE>
{referencesSituation = ReferencesSituationEnum.ON_DELETE.symbol(situationSpan.end(this));referencesOption =parseReferencesOption();}
]
{return new SqlConstraintReferences(span.end(this), referencesTable, referencesColumns, referencesSituation, referencesOption);}
}

SqlLiteral parseReferencesOption() :
{
Span span = Span.of(getPos());
}
{
<CASCADE> {return ReferencesOptionEnum.CASCADE.symbol(span.end(this));}
|
(<SET> <NULL>) {return ReferencesOptionEnum.SET_NULL.symbol(span.end(this));}
}

SqlNode parseConstraintCheckExpression() :
{
SqlNode checkCondition = null;
}
{
<LPAREN> checkCondition = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
{return checkCondition;}
}

void parseInlineRefConstraintDefinition() :
{
}
{
(<SCOPE> <IS> CompoundIdentifier())
|
(<WITH> <ROWID>)
}


void parsePeriodDefinition() :
{
}
{
<PERIOD> {throw new UnsupportedOperationException("unsupported period definition");}
}

void parseOutOfLineConstraintDefinition(SqlNodeList constraintList) :
{
SqlIdentifier constraintName = null;
SqlNodeList columnList = null;
SqlLiteral constraintSpec = null;
SqlConstraintReferences foreignReferences = null;
SqlNode checkCondition = null;
Span span = Span.of(getPos());
}
{
[<CONSTRAINT> constraintName = SimpleIdentifier()]
(
(<UNIQUE> {constraintSpec = SqlConstraintSpec.UNIQUE.symbol(span.end(this)); columnList = ParenthesizedSimpleIdentifierList();})
|
(<PRIMARY> <KEY> {constraintSpec = SqlConstraintSpec.PRIMARY_KEY.symbol(span.end(this)); columnList = ParenthesizedSimpleIdentifierList();})
|
(<FOREIGN> <KEY> {constraintSpec = SqlConstraintSpec.FOREIGN_KEY.symbol(span.end(this)); columnList = ParenthesizedSimpleIdentifierList();} <REFERENCES> {foreignReferences = parseReferencesSpec();})
|
(<CHECK> {constraintSpec = SqlConstraintSpec.CHECK.symbol(span.end(this)); checkCondition = Expression(ExprContext.ACCEPT_SUB_QUERY);})
)
// outOfLineRefConstraint
[<REF> {throw new UnsupportedOperationException("unsupported outOfLineRefConstraint");}]
[<SCOPE> <OF>{throw new UnsupportedOperationException("unsupported outOfLineRefConstraint");}]

{constraintList.add(new SqlConstraint(span.end(this), constraintName, columnList, constraintSpec, foreignReferences, checkCondition));}
}

void parseSupplementalLoggingPropsDefinition() :
{
}
{
<GROUP> {throw new UnsupportedOperationException("unsupported SupplementalLoggingPropsDefinition");}
}

void parsePhysicalProperties() :
{
}
{
(
(
[<SEGMENT> <CREATION> [<IMMEDIATE>|<DEFERRED>]]
(parseSegmentAttributesClause()|<ORGANIZATION>|<EXTERNAL>)
)
|
<CLUSTER>
)
}

void parseSegmentAttributesClause() :
{
}
{
parsePhysicalAttributeClause()
|
<TABLESPACE>
|
parseLoggingClause()
}

void parsePhysicalAttributeClause() :
{
}
{
<PCTFREE>
|
<PCTUSED>
|
<INITRANS>
|
<STORAGE>
}

void parseLoggingClause() :
{
}
{
<LOGGING>
|
<NOLOGGING>
|
<FILESYSTEM_LIKE_LOGGING>
}

void parseTableProperties() :
{
}
{
parseColumnProperties() (parseColumnProperties())*
[parseReadOnlyClause()]
[parseIndexClause()]
[parseTablePropertiesClause()]
[parseAttributeClusteringClause()]
[<CACHE>|<NOCACHE>]
[parseResultCacheClause()]
[parseParallelClause()]
[<ROWDEPENDENCIES>|<NOROWDEPENDENCIES>]
[parseEnableDisableClause()]
[parseFlashbackArchiveClause()]
[<ROW> <ARTRIVE>]
[<AS>|<FOR>]
}

void parseColumnProperties() :
{
}
{
(
// object
<OBJECT>
// nested
|<NESTED>
// varray
|<VARRAY>
// LOB
|<LOB>
// XML
|<XMLTYPE>
// json
|<JSON>
)
}

void parseReadOnlyClause() :
{
}
{
<READ>
}

void parseIndexClause() :
{
}
{
<INDEXING>
}

void parseTablePropertiesClause() :
{
}
{
<PARTITION>
//range_properties
//list_properties
//hash_properties
//composite_range_partitions
//composite_list_partitions
//composite_hash_partitions
//references_partitions
//system_partitioning
//consistent_hash_partitions
//consistent_hash_with_subpartitions
}

void parseAttributeClusteringClause() :
{
}
{
<CLUSTERING>
}

void parseResultCacheClause() :
{
}
{
<RESULT_CACHE>
}

void parseParallelClause() :
{
}
{
<NOPARALLEL>|<PARALLEL>
}

void parseEnableDisableClause() :
{
}
{
<ENABLE>|<DISABLE>
}

void parseFlashbackArchiveClause() :
{
}
{
<FLASHBACK>|<NO>
}

<#--parse table and column comment-->
SqlNode SqlCommon() :
{
SqlIdentifier commentIdentify = null;
SqlLiteral commentType = null;
String comment = null;
Span span = Span.of(getPos());
Span commentTypeSpan = null;
}
{
<COMMENT> <ON>
{commentTypeSpan = Span.of(getPos());}
(
<COLUMN>{commentType = CommentTypeEnum.COLUMN.symbol(commentTypeSpan.end(this));}
|
<TABLE>{commentType = CommentTypeEnum.TABLE.symbol(commentTypeSpan.end(this));}
)
commentIdentify = CompoundIdentifier()
<IS> <QUOTED_STRING> { comment = token.image;}
{return new SqlComment(span.end(this), commentIdentify, commentType, comment);}
}


SqlDrop SqlDropTable() :
{
SqlIdentifier tableIdentifier = null;
boolean isCascadeConstraints = false;
Span span = Span.of(getPos());
}
{
<DROP> <TABLE> tableIdentifier = CompoundIdentifier()
[<CASCADE> <CONSTRAINTS> {isCascadeConstraints = true;}]
[<PURGE>]
[<AS> SimpleIdentifier()]
{return new SqlDropTable(span.end(this), false, tableIdentifier, isCascadeConstraints);}
}

SqlTruncateTable SqlTruncateTable() :
{
SqlIdentifier tableIdentifier = null;
Span span = Span.of(getPos());
}
{
<TRUNCATE> <TABLE> tableIdentifier = CompoundIdentifier()
{return new SqlTruncateTable(span.end(this), tableIdentifier);}
}


SqlAlterTable SqlAlterTable() :
{
SqlAlterTable sqlAlterTable = null;
SqlIdentifier tableIdentifier = null;
Span span = Span.of(getPos());
}
{
<ALTER> <TABLE> tableIdentifier = CompoundIdentifier()
[<MEMOPTIMIZE>|(<NO> <MEMOPTIMIZE>) {throw new UnsupportedOperationException("unsupported MEMOPTIMIZE definition");}]
(
<RENAME>{sqlAlterTable = parseAlterRename(tableIdentifier, span);}
|
(
{SqlNodeList alterTableAddList = new SqlNodeList(getPos());}
<ADD>parseAlterAdd(tableIdentifier, alterTableAddList) (<ADD>parseAlterAdd(tableIdentifier, alterTableAddList))*
{sqlAlterTable = new SqlAlterTableAdd(span.end(this), tableIdentifier, alterTableAddList);}
)
|
(
{SqlNodeList alterTableDropList = new SqlNodeList(getPos());}
<DROP>{parseAlterDrop(tableIdentifier, alterTableDropList);} (<DROP>{parseAlterDrop(tableIdentifier, alterTableDropList);})*
{sqlAlterTable = new SqlAlterTableDrop(span.end(this), tableIdentifier, alterTableDropList);}
)
)
{return sqlAlterTable;}
}

SqlAlterTable parseAlterRename(SqlIdentifier tableIdentifier, Span span) :
{
SqlIdentifier oldIdentifier = null;
SqlIdentifier newIdentifier = null;
}
{
(
<TO>
{
newIdentifier = CompoundIdentifier();
return new SqlRenameTable(span.end(this), tableIdentifier, newIdentifier);
}
|
(
<COLUMN>
oldIdentifier = SimpleIdentifier()
<TO>
newIdentifier = SimpleIdentifier()
{return new SqlRenameColumn(span.end(this), tableIdentifier, oldIdentifier, newIdentifier);}
)
|
(
<CONSTRAINT>
oldIdentifier = SimpleIdentifier()
<TO>
newIdentifier = SimpleIdentifier()
{return new SqlRenameConstraint(span.end(this), tableIdentifier, oldIdentifier, newIdentifier);}
)
)
}

void parseAlterAdd(SqlIdentifier tableIdentifier, SqlNodeList alterTableAddList) :
{
}
{
(
<LPAREN> parseAlterAddColumn(tableIdentifier, alterTableAddList)
|
parseAlterAddConstraint(tableIdentifier, alterTableAddList)
)
}

void parseAlterAddColumn(SqlIdentifier tableIdentifier, SqlNodeList alterTableAddList) :
{
SqlNodeList columnList = new SqlNodeList(getPos());
SqlNodeList constraintList = new SqlNodeList(getPos());
Span span = Span.of(getPos());
}
{
parseColumnDefinition(columnList, constraintList) (<COMMA> parseColumnDefinition(columnList, constraintList))*<RPAREN>
{alterTableAddList.add(new SqlAddColumn(span.end(this), tableIdentifier, columnList, constraintList));}
}

void parseAlterAddConstraint(SqlIdentifier tableIdentifier, SqlNodeList alterTableAddList) :
{
SqlNodeList constraintList = new SqlNodeList(getPos());
Span span = Span.of(getPos());
}
{
parseOutOfLineConstraintDefinition(alterTableAddList)
}

void parseAlterDrop(SqlIdentifier tableIdentifier, SqlNodeList alterTableDropList) :
{
SqlIdentifier dropIdentifier = null;
}
{
(
<LPAREN> {parseParenColumnDrop(tableIdentifier, alterTableDropList);}
|
<COLUMN> {parseColumnDrop(tableIdentifier, alterTableDropList);}
|
parseConstraintDrop(tableIdentifier, alterTableDropList)
)
}

void parseParenColumnDrop(SqlIdentifier tableIdentifier, SqlNodeList alterTableDropList) :
{
Span span = Span.of(getPos());
SqlNodeList dropColumnList = new SqlNodeList(getPos());
}
{
{dropColumnList.add(SimpleIdentifier());} (<COMMA> {dropColumnList.add(SimpleIdentifier());})* <RPAREN>
{alterTableDropList.add(new SqlDropColumn(span.end(this), tableIdentifier, dropColumnList));}
}

void parseColumnDrop(SqlIdentifier tableIdentifier, SqlNodeList alterTableDropList) :
{
Span span = Span.of(getPos());
SqlNodeList dropColumnList = new SqlNodeList(getPos());
}
{
{dropColumnList.add(SimpleIdentifier());}(<COMMA> {dropColumnList.add(SimpleIdentifier());})*
{alterTableDropList.add(new SqlDropColumn(span.end(this), tableIdentifier, dropColumnList));}
}

void parseConstraintDrop(SqlIdentifier tableIdentifier, SqlNodeList alterTableDropList) :
{
Span span = Span.of(getPos());
SqlLiteral constraintSpec = null;
SqlIdentifier constraintIdentifier = null;
SqlNodeList columnNameList = null;
}
{
(
(<PRIMARY> <KEY> {constraintSpec = SqlLiteral.createSymbol(SqlConstraintSpec.PRIMARY_KEY, getPos());})
|
(
<UNIQUE>
{
columnNameList = new SqlNodeList(getPos());
constraintSpec = SqlLiteral.createSymbol(SqlConstraintSpec.UNIQUE, getPos());
}
<LPAREN> {columnNameList.add(SimpleIdentifier());} (<COMMA> {columnNameList.add(SimpleIdentifier());})* <RPAREN>
)
|
<CONSTRAINT>
{
constraintSpec = SqlLiteral.createSample(SqlSampleSpec.createNamed("CONSTRAINT"), getPos());
constraintIdentifier = SimpleIdentifier();
}
)
[<CASCADE>]

[LOOKAHEAD(2)(<KEEP>|<DROP>) <INDEX>]
[<ONLINE>]
{alterTableDropList.add(new SqlDropConstraint(span.end(this), tableIdentifier, constraintSpec, constraintIdentifier, columnNameList));}
}

SqlCreateIndex SqlCreateIndex() :
{
Span s = Span.of(getPos());
IndexType indexType = null;
SqlIdentifier indexIdentifier = null;
}
{
<CREATE>
[
<UNIQUE> {indexType = IndexType.UNIQUE;}
|<BITMAP> {throw new UnsupportedOperationException("BITMAP index is not supported yet.");}
|<MULTVALUE>{throw new UnsupportedOperationException("MULTVALUE index is not supported yet.");}
]
<INDEX>
indexIdentifier = CompoundIdentifier()
[<ILM> {throw new UnsupportedOperationException("ILM index is not supported yet.");}]
<ON>
[<CLUSTER> {throw new UnsupportedOperationException("CLUSTER index is not supported yet.");}]
{return parseIndexTable(s, indexType, indexIdentifier);}
}

SqlCreateIndex parseIndexTable(Span s, IndexType indexType, SqlIdentifier indexIdentifier) :
{
SqlIdentifier tableIdentifier = null;
SqlIdentifier aliasIdentifier = null;
SqlNodeList indexColumnList = new SqlNodeList(getPos());
}
{
tableIdentifier = CompoundIdentifier()
[aliasIdentifier = SimpleIdentifier()]
<LPAREN> {indexColumnList.add(CompoundIdentifier());} (<COMMA> {indexColumnList.add(CompoundIdentifier());})* <RPAREN>
[<NOPARALLEL>]
{return new SqlCreateTableIndex(s.end(this), indexType, indexIdentifier, tableIdentifier, aliasIdentifier, indexColumnList);}
}

SqlAlterIndex SqlAlterIndex() :
{
Span s = Span.of(getPos());
SqlIdentifier indexIdentifier = null;
}
{
<ALTER> <INDEX> indexIdentifier = CompoundIdentifier()
[<ILM> {throw new UnsupportedOperationException("ILM index is not supported yet.");}]
{return parseRenameIndex(s, indexIdentifier);}
}

SqlAlterIndex parseRenameIndex(Span s, SqlIdentifier indexIdentifier) :
{
SqlIdentifier newIdentifier = null;
}
{
<RENAME> <TO> newIdentifier = CompoundIdentifier()
{return new SqlRenameIndex(s.end(this), indexIdentifier, newIdentifier);}
}

SqlDropIndex SqlDropIndex() :
{
Span s = Span.of(getPos());
SqlIdentifier indexIdentifier = null;
}
{
<DROP> <INDEX> indexIdentifier = CompoundIdentifier()
{return new SqlDropIndex(s.end(this), indexIdentifier);}
}

