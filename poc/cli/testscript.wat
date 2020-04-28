put wat/1 "hi": "wat"

put wat/1 "bye" :"watter" 

put wat/2 "hi":"bat"
put wat/2 "bye" :"batter" 
put austin 
        "name":"Austin",
        "age": 38,  
        "child":@"gwynneth",
        "child":@"august",
        "child":@"blakely",
        "spouse":@"kendra",
        "friend":@"alan";
    kendra 
        "name":"Kendra", 
        "age": 32,    
        "child":@"gwynneth",
        "child":@"august",
        "child":@"blakely",
        "spouse":@"austin",
        "friend":@"samantha";
    gwynneth 
        "name":"Gwynneth", 
        "age": 5,    
        "mother":@"kendra",
        "brother":@"august",
        "sister":@"blakely",
        "father":@"austin";
    blakely 
        "name":"Blakely", 
        "age": 3,    
        "mother":@"kendra",
        "brother":@"august",
        "sister":@"gwynneth",
        "father":@"austin";
    august 
        "name":"August", 
        "age": 2,
        "mother":@"kendra",
        "sister":@"blakely",
        "sister":@"gwynneth",
        "father":@"austin";
    alan
        "name":"Alan", 
        "age": 2,
        "spouse":@"samantha",
        "friend":@"austin";
    samantha
        "name":"Samantha", 
        "age": 2,
        "spouse":@"alan",
        "friend":@"kendra";
