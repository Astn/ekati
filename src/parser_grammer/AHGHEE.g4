// this is the comment

grammar AHGHEE;

command
   : put
   | get
   | load
   ;

put  
    : 'put' flags? node (';' node)*
    ;   
         
get  
    : 'get' flags? nodeid (',' nodeid)* pipe?
    ;

load
    : 'load' loadtype loadpath 
    ;

nodeid
    : obj 
    | remote? id
    ;

loadpath: STRING;
loadtype: WORD;

remote: (WORD | STRING);
id: (WORD | STRING) ;

flags
    : '-' WORD; 

/// JSON GRAMMER

node
   : obj
   | nodeid (obj|kvps)
   ;

obj
   : '{' kvps '}'
   | '{' '}'
   ;



kvps: pair (',' pair)*;

pair
   : kvp
   | edge
   | fedge
   | dedge
   ;
   
kvp
    : STRING ':' value
    ;

edge
    : STRING ':' '^' STRING
    ;

fedge
    :  '^' STRING':' value
    ;

dedge
    : '^' STRING ':' '^' STRING
    ;


arr
   : '[' value (',' value)* ']'
   | '[' ']'
   ;

value
   : STRING
   | NUMBER
   | obj
   | arr
   | 'true'
   | 'false'
   | 'null'
   ;

pipe
    : PIPESTART pipecmd pipe?
    ;
    
pipecmd
    : follow
    | wherefilter
    | limitfilter
    | skipfilter
    | fields
    | exclude
    ;   

fields
    : 'fields' clude 
    ;
    
exclude
    : '-' clude
    ; 
include
    : '+' clude
    ;       

clude
    : '(' clude (',' clude)* ')'  
    | clude exclude
    | clude include
    | exclude
    | include
    | cludeop
    ;

skipfilter
    : 'skip' NUMBER
    | 'offset' NUMBER
    ;

limitfilter
    : 'limit' NUMBER
    | 'take' NUMBER
    ;

wherefilter
    : 'filter' compare
    ;
follow
    : 'follow' (anynum | edgenum )
    ;         
compare
    : '(' compare BOOLOP compare ')'
    |  wfkey MATHOP wfvalue
    | '(' wfkey MATHOP wfvalue ')'
    ;    
edgenum
    : value range?
    | '(' value range? ')'
    | '(' edgenum BOOLOP edgenum ')'
    ;    
wfkey: value;
wfvalue: value;
range
    : ((from '..')? to)
    ;
from: NUMBER;
to: NUMBER;    


anynum
    : '*' range?
    ;    
    
cludeop
     : cludepart ':' cludepart
     ;  
cludepart
    : CARET STRING 
    | CARET 
    | STRING 
    | STAR 
    | TYPEINT 
    | TYPESTRING 
    | TYPEFLOAT
    ;
         
STAR : '*';
CARET : '^';      
TYPEINT : 'int';
TYPESTRING: 'string';  
TYPEFLOAT : 'float'; 

MATHOP
    : '=='|'<'|'<='|'>='|'>'|'!=' ;    
BOOLOP
    : '&&' | '||' ;

PIPESTART : '|>' ;

STRING
   : '"' (ESC | SAFECODEPOINT | WORD)* '"'
   ;

WORD
    : [A-Za-z]+[A-Za-z/0-9#?&:.=]*;

NUMBER
   : '-'? INT ('.' [0-9] +)? EXP?
   ;
   
fragment ESC
   : '\\' (["\\/bfnrt] | UNICODE)
   ;
fragment UNICODE
   : 'u' HEX HEX HEX HEX
   ;
fragment HEX
   : [0-9a-fA-F]
   ;
fragment SAFECODEPOINT
   : ~ ["\\\u0000-\u001F]
   ;
fragment SAFECODEPOINTNOSPACE
   : ~ ["\\\u0000-\u0020]
   ;
   
fragment INT
   : '0' | [1-9] [0-9]*
   ;

fragment EXP
   : [Ee] [+\-]? INT
   ;

COMMENT
    : '/*' .*? '*/' -> skip //channel(COMMENTS)
    ;

LINE_COMMENT
    : '//' ~[\r\n]* -> skip //channel(COMMENTS)
    ;
WS
   : [ \t\n\r] + -> skip
   ;

//mode ARROW;

