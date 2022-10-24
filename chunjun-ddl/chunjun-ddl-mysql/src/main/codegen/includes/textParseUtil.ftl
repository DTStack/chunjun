SqlNode ParseStringWithStartNumber():
{
String p  ;
}
{
<DECIMAL_NUMERIC_LITERAL>{
    p = token.image + SimpleIdentifierValue();

    return SqlStringLiteral(p, getPos());
    }
    |
    <UNSIGNED_INTEGER_LITERAL>{
        p = token.image + SimpleIdentifierValue();
        return SqlStringLiteral(p, getPos());
        }
        }


        JAVACODE String StringLiteralValue() {
        SqlNode sqlNode = StringLiteral();
        return ((NlsString) SqlLiteral.value(sqlNode)).getValue();
        }

        JAVACODE String SimpleIdentifierValue() {
        SqlNode sqlNode = SimpleIdentifier();
        return sqlNode.toString();
        }


        JAVACODE SqlLiteral SqlStringLiteralBySimpleIdentifier () {
        SqlIdentifier sqlNode = SimpleIdentifier();
        NlsString slit = new NlsString(sqlNode.toString(), null, null);
        return new SqlStringLiteral(slit, sqlNode.getParserPosition());
        }


        JAVACODE SqlLiteral SqlStringLiteral (String s, SqlParserPos pos) {
        NlsString slit = new NlsString(s, null, null);
        return new SqlStringLiteral(slit, pos);
        }
