get -abc "name/name/name" 
    |> follow (("knows" 5 || "wat" 2) && "fat" 5)
    |> follow "fdas" 6
    |> filter (
        (("occupation" == "software engineer" && "age" == 38)
        || "occupation" == "engineer")
        && ("age" == 38 && "city" == "Portland"))
    |> follow "friend" 1
