module CD.Program

open GetCubeVersion 
open CubeConnection  
open RemoveUnwantedDimensions

let main() = 
    let ver = getCubeVer 
    let cc =  getCubeConnection ("TOPSDEV03", "MI_R_A6","cub_MI")
    match ver with
        | 8 -> removeUnwantedDimensions cc  


    0

main()
