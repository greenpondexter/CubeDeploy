module RemoveUnwantedDimensions

open DataConnections.SqlConnection
open DataConnections.Tables
open System.Data
open System.Data.Linq
open DataConnections.CubeConnection 
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 

let miParmsTable = miParmsTable

type ParmConfig = {
       claim : bool; 
       meg   : bool;
       etg   : bool;
       }

type UpdateType = 
    |CUBE
    |DATABASE
    |BOTH

type ActionType = 
    |DB_DIMENSION  
    |CUBE_DIMENSION
    |MEASURE_GROUP  
    |MEASURE 

type ActionInput = 
   | Sng of string list
   | Dbl of (string * string) list  

let miParmQuery parm : bool = 
    let res : seq<bool> =
        query {
            for row in miParmsTable do
            where (row.Parm = parm)
            select row.Setting
        } 
        |> Seq.map (fun res -> if (res = "" || res = "NO") then true else false)
    Seq.head res 

let getSettings  = 
    let sts = 
        let q = ["SSAS INCLUDE CLAIM DIM";"MEG ENABLED";"ETG ENABLED";]
        q
        |> List.map(fun q -> miParmQuery q) 
    { claim=sts.Item(0); meg=sts.Item(1); etg=sts.Item(2);}

let updateSource (updType: UpdateType) (con:Cube) =
    match updType with
    |CUBE       -> con.Update(UpdateOptions.ExpandFull, UpdateMode.Default)
    |DATABASE   -> con.Update(UpdateOptions.ExpandFull, UpdateMode.Default)
    |BOTH       -> con.Update(UpdateOptions.ExpandFull, UpdateMode.Default)
                   con.Parent.Update(UpdateOptions.ExpandFull, UpdateMode.Default)       

let removeMeasure (upd:(string * string)) (con:Cube) =
    let (mg,m) = upd
    try con.MeasureGroups.GetByName(mg).Measures.Remove(m)
    with | :? Microsoft.AnalysisServices.AmoException -> printfn "Couldn't find MeasureGroup: %s" (fst upd)

let removeDimension (dim:string) (con:Cube) = 
    try con.Dimensions.Remove(dim) 
    with | :? Microsoft.AnalysisServices.AmoException -> printfn "Couldn't find Dimension: %s" dim

let removeMeasureGroup (mg:string) (con:Cube) =
    try con.MeasureGroups.Remove(mg)
    with | :? Microsoft.AnalysisServices.AmoException -> printfn "Cound find MeasureGroup: %s" mg

let removeItem (items:ActionInput) (action:ActionType) (con: Cube) : Cube =

    match items with
    |Sng(items) -> 
        match action with
            |CUBE_DIMENSION -> 
                items |> List.map (fun (dim:string) -> removeDimension dim con)
            |DB_DIMENSION -> 
                items |> List.map (fun (dim:string) -> removeDimension dim con)
            |MEASURE_GROUP ->
                items |> List.map (fun (mg:string) -> removeMeasureGroup mg con)
    |Dbl(items) -> items |> List.map (fun (upd:string*string) ->  con.MeasureGroups.GetByName(fst upd).Measures.Remove(snd upd))
    con 

let updateItem (items:(string * string) list) (action:ActionType) (con:Cube) : Cube =
    
    match action with
    |DB_DIMENSION -> 
        items |> List.map (fun (upd:string*string) -> con.Parent.Dimensions.GetByName(fst upd).Name = snd upd ) 
    |CUBE_DIMENSION -> 
        items |> List.map (fun (upd:string*string) -> con.Dimensions.GetByName(fst upd).Name = snd upd ) 
    |MEASURE_GROUP ->
        items |> List.map (fun (upd:string*string) -> con.MeasureGroups.GetByName(fst upd).Name = snd upd)
    |MEASURE ->
        items |> List.map (fun (upd:string*string) -> con.MeasureGroups.GetByName(fst upd).Name = snd upd)
    con

let dropEtg (cConn : Cube) = 
    
    let dR = ["ddProvider_ETG_Attrib";
    "ddProvider_PCP 1";
    "ddProvider_PCP_Attrib 1";
    "ddMemberInfo";
    "ddUDD 1";
    "ddCCHGCategory 1";
    "ddDate_EpisodeEnd";
    "ddETG"]
    
    cConn
    |> removeItem  (Sng(["RPT ETG";"ddProvider_ETG_Attrib"])) DB_DIMENSION
    |> removeItem (Dbl([("Services","ETG EPISODE COUNT ALLOWED PRORATE");])) MEASURE
    |> removeItem (Sng(["RPX ETG PROFILING"])) MEASURE_GROUP
    |> updateItem ([("ddMeg", "Episode")]) DB_DIMENSION 
    |> updateItem ([("RPX MEG PROFILING","Episode Profiling")])  MEASURE_GROUP 
    |> removeItem (Sng(dR)) CUBE_DIMENSION 
    |> removeItem (Sng(["RPX ETG PROFILING"])) MEASURE_GROUP
    
    updateSource BOTH cConn

let dropMeg (cConn : Cube) =
    
    let dR = ["ddProvider_PCP 2";"ddProvider_PCP_Attrib 2";"ddMemberInfo 1";"ddUDD 2";"ddCCHGCategory 2";"ddDate_EpisodeEnd 1";
              "ddETG";"ddProvider_ETG_Attrib";"ddProvider_PCP 1";"ddProvider_PCP_Attrib 1";"ddMemberInfo";"ddUDD 1";
              "ddCCHGCategory 1";]
    
    cConn
    |> removeItem (Dbl([("Services","MEG EPISODE COUNT ALLOWED PRORATE");])) MEASURE
    |> removeItem (Sng(["ddETG";"ddProvider_ETG_Attrib";])) DB_DIMENSION
    |> removeItem (Sng(dR)) CUBE_DIMENSION 
    |> updateItem [("ddMEG", "Episode");] CUBE_DIMENSION 

    let getMeasureGroupAttribute (c: MeasureGroupDimension) = 
        let m = c :?> RegularMeasureGroupDimension
        m.Attributes
        |> Seq.cast<MeasureGroupAttribute>
        |> Seq.find (fun i -> i.AttributeID  = "daDate")

    
    let getMeasureGroupKeyColumn (e:MeasureGroupAttribute) =
        e.KeyColumns
        |> Seq.cast<DataItem>
        |> Seq.head 

    let updateKeyColumnBinding (f:DataItem) =
        let n = f.Source :?> ColumnBinding
        n.ColumnID = "etg_max_incurred_date" 
        
                       
    cConn.MeasureGroups.GetByName("Services").Dimensions
            |> Seq.cast<MeasureGroupDimension>
            |> Seq.find (fun dim -> dim.CubeDimensionID = "ddDate_EpisodeEnd")
            |> getMeasureGroupAttribute 
            |> getMeasureGroupKeyColumn
            |> updateKeyColumnBinding 
    true 
    
    updateSource BOTH cConn
    
let neitherEpsEnabled (cConn : Cube) =
    
    let dR = [
        "ddMemberInfo 1";
        "ddCCHGCategory 2";
        "ddUDD 2";
        "ddProvider_PCP 2";
        "ddDate_EpisodeEnd 1";
        "ddProvider_PCP_Attrib 2";
        "ddETG";
        "ddProvider_ETG_Attrib";
        "ddProvider_PCP 1";
        "ddProvider_PCP_Attrib 1";
        "ddMemberInfo";
        "ddUDD 1";
        "ddCCHGCategory 1";
        "ddDate_EpisodeEnd";]
    
    cConn
    |> removeItem (Sng(dR)) DB_DIMENSION                               
    |> removeItem (Dbl([("Services","ETG EPISODE COUNT ALLOWED PRORATE");("Services", "MEG EPISOEDE COUNT ALLOWED PRORATE");])) MEASURE
    |> removeItem (Sng(["RPX ETG PROFILING";"RPX MEG PROFILING"])) MEASURE_GROUP
    |> updateItem [("ddMEG", "Episode");] CUBE_DIMENSION 

    updateSource BOTH cConn

let removeUnwantedDimensions (cConn : Cube ) = 
    let settings = getSettings 
    match settings with
    | {meg = true; etg = false; }   -> dropEtg cConn   
    | {meg = false; etg = true; }   -> dropMeg cConn
    | {meg = false; etg = false; }  -> neitherEpsEnabled cConn 
