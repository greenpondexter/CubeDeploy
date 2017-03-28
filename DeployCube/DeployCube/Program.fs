module CD.Program

open DataConnections.CubeConnection  
open GetCubeVersion 
open RemoveUnwantedDimensions
open CreateAggregations
open CreatePerspectives 
open UpdateMeasureNames
open Ops

let main() = 

    let ver = getCubeVer 
    let cc =  getCubeConnection ("TOPSDEV03", "MI_R_A6","cub_MI")
    match ver with
        | 8 -> updateMeasureNames cc 
               removeUnwantedDimensions cc
               createAggregations cc
               createPerspectives cc
                
    0

main()
