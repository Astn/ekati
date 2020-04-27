put wat/1 "hi": "wat"
get wat/1
put wat/1 "bye" :"watter" 
get wat/1
put wat/2 "hi":"bat"
put wat/2 "bye" :"batter" 
get wat/2
get wat/1
get wat/2, wat/1 |> filter "str" ==  "watter" 
put austin 
        "name":"Austin",
        "age": 38,  
        "child":@"gwynneth",
        "child":@"august",
        "child":@"blakely",
        "spouce":@"kendra";
    kendra 
        "name":"Kendra", 
        "age": 32,    
        "child":@"gwynneth",
        "child":@"august",
        "child":@"blakely",
        "spouce":@"austin";
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
            "spouce":@"samantha",
            "friend":@"austin";
    samantha
        "name":"Samantha", 
            "age": 2,
            "spouce":@"alan",
            "friend":@"kendra";