module GetCubeVersion 

open SqlConnection
open System.Data
open System.Data.Linq
open Microsoft.FSharp.Data.TypeProviders
open Microsoft.FSharp.Linq

let getCubeVer = 
    db.ASRPT_FN_GET_CUBE_VER().GetValueOrDefault()
        
        
