module CD.Program

open DataConnections.CubeConnection  
open GetCubeVersion 
open RemoveUnwantedDimensions
open CreateAggregations
open CreatePerspectives 
open Ops

let main() = 

//    let  add1 x = x + 1
//    let (<*>) = List.apply
//    let res = [add1] <*> [1;2;] 
    let ver = getCubeVer 
    let cc =  getCubeConnection ("TOPSDEV03", "MI_R_A6","cub_MI")
    match ver with
        | 8 ->  createPerspectives cc

    0

//                removeUnwantedDimensions cc
//                createAggregations cc  
main()
