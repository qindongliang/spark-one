grammar SparkOneDsl;

script
    : terminator* (statement terminator*)* EOF
    ;

statement
    : loadStatement
    | saveStatement
    | sqlStatement
    ;

loadStatement
    : LOAD source optionClause? AS table=identifier
    ;

saveStatement
    : SAVE saveMode? table=identifier AS source optionClause?
    ;

source
    : format=identifier DOT path=quotedValue
    ;

optionClause
    : (WHERE | OPTIONS) option (AND option)*
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

quotedValue
    : BACKQUOTED_IDENTIFIER
    | SINGLE_QUOTED_STRING
    | DOUBLE_QUOTED_STRING
    ;

identifier
    : IDENTIFIER
    ;

sqlStatement
    : sqlToken+
    ;

sqlToken
    : LOAD
    | SAVE
    | AS
    | WHERE
    | OPTIONS
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
    | SQL_CHAR
    ;

terminator
    : SEMICOLON+
    ;

LOAD: L O A D;
SAVE: S A V E;
AS: A S;
WHERE: W H E R E;
OPTIONS: O P T I O N S;
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
SEMICOLON: ';';

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
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
