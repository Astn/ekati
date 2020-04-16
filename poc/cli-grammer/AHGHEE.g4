// this is the comment

grammar AHGHEE;


command
   : put
   | get
   ;

put  
    : 'put' flags? node (';' node)*
    ;   
         
get  
    : 'get' flags? nodeid (',' nodeid)*
    ;
     
nodeid
    : obj 
    | remote? id
    ;
    

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
   : STRING ':' value
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

STRING
   : '"' (ESC | SAFECODEPOINT | WORD)* '"'
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

WORD
    : [A-Za-z]+[A-Za-z/0-9#?&:.=]*;

NUMBER
   : '-'? INT ('.' [0-9] +)? EXP?
   ;


fragment INT
   : '0' | [1-9] [0-9]*
   ;

// no leading zeros

fragment EXP
   : [Ee] [+\-]? INT
   ;

// \- since - means "range" inside [...]

WS
   : [ \t\n\r] + -> skip
   ;