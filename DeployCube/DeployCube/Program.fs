module CD.Program

open GetCubeVersion 
open CubeConnection  
open RemoveUnwantedDimensions
open CreateAggregations
open Operators

let main() = 

    let  add1 x = x + 1
    let (<*>) = List.apply
    let res = [add1] <*> [1;2;] 
    let ver = getCubeVer 
    let cc =  getCubeConnection ("TOPSDEV03", "MI_R_A6","cub_MI")
    match ver with
        | 8 ->  removeUnwantedDimensions cc
                createAggregations cc  

    0

main()
