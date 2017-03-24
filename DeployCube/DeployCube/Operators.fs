module Operators

    module List =
                
    let apply (fList: ('a->'a) list) (valList: 'a list) = 
        fList 
        |> List.collect (fun x -> valList 
                                  |> List.map (fun y -> x y))