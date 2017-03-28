module UpdateHierarchyNames

open DataConnections.SqlConnection
open DataConnections.CubeConnection 
open DataConnections.Tables 

open System.Data 
open System.Data.Linq
open DataConnections.CubeConnection 
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 
open Ops

let hierTable = hierTable
let hierLevelTable = hierLevelTable

type HierInfo = {
    dimId: string;
    dimHierId: string;
    dimHierName: string;
    dimHierDisplayFolder: string;
    dimHierTransName: string;
    dimHierTransDisplayFolder: string;
}

type HierLevelInfo = {
    dimId: string;
    dimHierId: string;
    dimHierLevelId: string;
    dimHierLevelName: string;
    dimHierLevelTransName: string;
}

let hierQuery : seq<HierInfo> =
    query {
        for row in hierTable do
        select row
    } |> Seq.map (fun row -> {
                                dimId = row.DimensionID;
                                dimHierId = row.DimensionHierarchyID;
                                dimHierName = row.DimensionHierarchyName;
                                dimHierDisplayFolder = row.DimensionHierarchyDisplayFolder;
                                dimHierTransName = row.DimensionHierarchyTranslatedName;
                                dimHierTransDisplayFolder = row.DimensionHierarchyTranslatedDisplayFolder;
                             })

let hierLevelQuery : seq<HierLevelInfo> =
    query {
        for row in hierLevelTable do
        select row
    } |> Seq.map (fun row -> {
                                dimId = row.DimensionID;
                                dimHierId = row.DimensionHierarchyID;
                                dimHierLevelId = row.DimensionHierarchyLevelID;
                                dimHierLevelName = row.DimensionHierarchyLevelName;
                                dimHierLevelTransName = row.DimensionHierarchyLevelTranslatedName;
                             })

let updateHierarchy (con: Cube) (hierInfo : HierInfo) = 
    
    let dim = con.Parent.Dimensions.Find(hierInfo.dimId)
    let hier = dim.Hierarchies.Find(hierInfo.dimHierId)

    let hierCaption = 
        let res = hierInfo.dimHierTransName.Length > 0 
        match res with
        |true   -> hierInfo.dimHierTransName
        |false  -> hierInfo.dimHierName

    let hierDisplayFolder = 
        let res = hierInfo.dimHierTransDisplayFolder.Length > 0
        match res with
        |true   -> hierInfo.dimHierTransDisplayFolder
        |false  -> hierInfo.dimHierDisplayFolder
        
    let hierTrans = hier.Translations.Item(0)
    match hierTrans with
    |null   -> printfn "No Translation Object for Hierarchy: %s" hier.Name
               false
    |_      -> hier.Translations.Item(0).Caption = hierCaption
               hier.Translations.Item(0).DisplayFolder = hierDisplayFolder
     

let updateHierLevel (con: Cube) (hierLevelInfo: HierLevelInfo) =
    
    let dim = con.Parent.Dimensions.Find(hierLevelInfo.dimId)
    let hier = dim.Hierarchies.Find(hierLevelInfo.dimHierId)
    let level = hier.Levels.Find(hierLevelInfo.dimHierLevelId)

    let hierLevelCaption =
        let res = hierLevelInfo.dimHierLevelTransName.Length > 0
        match res with
        |true   -> hierLevelInfo.dimHierLevelTransName
        |false  -> hierLevelInfo.dimHierLevelName

    let hierLevelTrans = level.Translations.Item(0)
    match hierLevelTrans with
    |null   ->  printfn "No Translation Object for Hierarchy Level: %s" level.ID
                false
    |_      ->  level.Translations.Item(0).Caption = hierLevelCaption

let updateHierarchyNames (cConn: Cube) = 
    
    let hierInfo = hierQuery
    Seq.toList hierInfo
    |> List.map (fun hier -> updateHierarchy cConn hier )

    let hierLevelInfo = hierLevelQuery
    Seq.toList hierLevelInfo
    |> List.map (fun hierLevel -> updateHierLevel cConn hierLevel)

    cConn.Parent.Dimensions
    |> Seq.cast<Dimension>
    |> Seq.map (fun dim -> dim.Update())