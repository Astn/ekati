// this is the comment

grammar NTRIPLES;

triples
    : triple+
    ;
    
triple
    : subj pred obj '.' COMMENT?  
    ;

subj
    : IRI
    | BLANKNODE
    ;

pred : IRI
    ;    

obj
   : IRI
   | BLANKNODE
   | literal
   ;

literal
    : STRING ('^^' IRI |  LANGTAG)?
    ;

BLANKNODE
    : STARTBLANKNODE BLANKNODELABEL+
    ;

LANGTAG
 	: STARTLANGTAG [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*
 	;

IRI
    : OPENIRI (ESC | SAFECODEPOINTNOSPACE | WORD)* CLOSEIRIR
    ;

STRING
   : '"' (ESC | SAFECODEPOINT | WORD)* '"'
   ;

fragment OPENIRI
    : '<'
    ;
fragment CLOSEIRIR
    : '>'
    ;

fragment STARTLANGTAG
    : '@'
    ;        
fragment STARTBLANKNODE
    : '_:'
    ;
fragment BLANKNODELABEL
    : [A-Z] 
    | [a-z] 
    | [\u00C0-\u00D6] 
    | [\u00D8-\u00F6] 
    | [\u00F8-\u02FF] 
    | [\u0370-\u037D] 
    | [\u037F-\u1FFF] 
    | [\u200C-\u200D] 
    | [\u2070-\u218F] 
    | [\u2C00-\u2FEF] 
    | [\u3001-\uD7FF] 
    | [\uF900-\uFDCF] 
    | [\uFDF0-\uFFFD] 
    | [\u10000-\uEFFFF]
    ;

   
fragment WORD
    : [A-Za-z]+[A-Za-z/0-9#?&:.=]*;
   
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

fragment COMMENT : '#' .*?  -> skip;
   
WS
   : [ \t\r\n] + -> skip
   ;
//mode ARROW;

   