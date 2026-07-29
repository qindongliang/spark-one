grammar SparkOneDsl;

script
    : terminator* (statement terminator*)* EOF
    ;

statement
    : loadStatement
    | saveStatement
    | setStatement
    | viewStatement
    | assertStatement
    | sqlStatement
    ;

loadStatement
    : LOAD source whereClause? optionClause? AS table=identifier
    ;

saveStatement
    : SAVE saveMode? table=identifier AS source optionClause? partitionClause?
    ;

viewStatement
    : VIEW table=identifier AS sqlStatement
    ;

setStatement
    : SET key=identifier (EQ value=setValue | AS query=sqlStatement)
    ;

assertStatement
    : ASSERT (table=identifier | LPAREN query=sqlStatement RPAREN)
      WHERE predicate=quotedValue MESSAGE message=quotedValue
      (ON FAILURE failureAction=assertionFailureAction)?
    ;

assertionFailureAction
    : FAIL
    | STOP
    ;

source
    : format=identifier DOT path=quotedValue
    ;

optionClause
    : OPTIONS option (AND option)*
    ;

whereClause
    : WHERE condition=quotedValue
    ;

partitionClause
    : PARTITIONBY identifier (COMMA identifier)*
    ;

option
    : key=identifier EQ value=optionValue
    ;

saveMode
    : OVERWRITE
    | APPEND
    | ERRORIFEXISTS
    | IGNORE
    ;

optionValue
    : quotedValue
    | identifier
    | NUMBER
    ;

setValue
    : quotedValue
    | identifier
    | NUMBER
    ;

quotedValue
    : BACKQUOTED_IDENTIFIER
    | SINGLE_QUOTED_STRING
    | DOUBLE_QUOTED_STRING
    ;

identifier
    : IDENTIFIER
    ;

sqlStatement
    : sqlElement+
    ;

sqlElement
    : sqlToken
    | parenthesizedSql
    ;

parenthesizedSql
    : LPAREN sqlElement* RPAREN
    ;

sqlToken
    : LOAD
    | SAVE
    | SET
    | VIEW
    | ASSERT
    | MESSAGE
    | ON
    | FAILURE
    | FAIL
    | STOP
    | AS
    | WHERE
    | OPTIONS
    | PARTITIONBY
    | AND
    | OVERWRITE
    | APPEND
    | ERRORIFEXISTS
    | IGNORE
    | IDENTIFIER
    | BACKQUOTED_IDENTIFIER
    | SINGLE_QUOTED_STRING
    | DOUBLE_QUOTED_STRING
    | NUMBER
    | DOT
    | EQ
    | COMMA
    | SQL_CHAR
    ;

terminator
    : SEMICOLON+
    ;

LOAD: L O A D;
SAVE: S A V E;
SET: S E T;
VIEW: V I E W;
ASSERT: A S S E R T;
MESSAGE: M E S S A G E;
ON: O N;
FAILURE: F A I L U R E;
FAIL: F A I L;
STOP: S T O P;
AS: A S;
WHERE: W H E R E;
OPTIONS: O P T I O N S;
PARTITIONBY: P A R T I T I O N B Y;
AND: A N D;
OVERWRITE: O V E R W R I T E;
APPEND: A P P E N D;
ERRORIFEXISTS: E R R O R I F E X I S T S;
IGNORE: I G N O R E;

IDENTIFIER: [A-Za-z_] [A-Za-z0-9_]*;
NUMBER: [0-9]+ ('.' [0-9]+)?;
BACKQUOTED_IDENTIFIER: '`' ('``' | ~'`')* '`';
SINGLE_QUOTED_STRING: '\'' ('\\' . | ~['\\])* '\'';
DOUBLE_QUOTED_STRING: '"' ('\\' . | ~["\\])* '"';

DOT: '.';
EQ: '=';
COMMA: ',';
SEMICOLON: ';';
LPAREN: '(';
RPAREN: ')';

LINE_COMMENT: '--' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .*? '*/' -> skip;
WS: [ \t\r\n]+ -> skip;

SQL_CHAR: .;

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
