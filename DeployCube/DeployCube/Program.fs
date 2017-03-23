module CD.Program

open GetCubeVersion 
open CubeConnection  
open RemoveUnwantedDimensions
open CreateAggregations

let main() = 
    let ver = getCubeVer 
    let cc =  getCubeConnection ("TOPSDEV03", "MI_R_A6","cub_MI")
    match ver with
        | 8 ->  removeUnwantedDimensions cc
                createAggregations cc  

    0

main()
