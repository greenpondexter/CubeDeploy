module UpdateMeasureNames

open DataConnections.SqlConnection
open DataConnections.CubeConnection 
open DataConnections.Tables 

open System.Data 
open System.Data.Linq
open DataConnections.CubeConnection 
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 
open Ops

let measTable = measTable

type MeasureInfo = {
    measId: string;
    measGrpId: string;
    measName: string;
    measDisplayFolder: string;
    measTransName: string;
    measTransDisplayFolder: string;
    includeIn: bool; 
}

let measQuery : seq<MeasureInfo> =
    query {
        for row in measTable do
        select row
    } |> Seq.map (fun row -> {
                                measId = row.MeasureID;
                                measGrpId = row.MeasureGroupID;
                                measName = row.MeasureName;
                                measDisplayFolder = row.MeasureDisplayFolder;
                                measTransName = row.MeasureTranslatedName;
                                measTransDisplayFolder = row.MeasureTranslatedDisplayFolder;
                                includeIn = row.IncludeInCube.GetValueOrDefault();        
                             })

let checkMeasureExistence (con:Cube) (meas: MeasureInfo) (measCon: MeasureGroup * Measure)=

    let (_, measure) = measCon
      
    let measCaption = 
        let res = meas.measTransName.Length > 0
        match res with
        |true   -> meas.measTransName
        |false  -> meas.measName

    let measDisplayFolder =
        let res =  meas.measDisplayFolder.Length > 0
        match res with
        |true   -> meas.measTransDisplayFolder
        |false  -> meas.measDisplayFolder

    let measTrans = measure.Translations.Item(0)
    match measTrans with
    |null -> printfn "No Translation Object for Measure: %s" measure.Name
             false
    |_ -> measure.Translations.Item(0).Caption = measCaption
          measure.Translations.Item(0).DisplayFolder = measDisplayFolder

let updateMeasure (con: Cube) (meas: MeasureInfo) =
    

    let mg = con.MeasureGroups.Find(meas.measGrpId)
    let measure = mg.Measures.Find(meas.measId)

    match measure with
    |null -> printfn "Measure doesn't exist in Cube: %s" meas.measName
             false
    |_    -> checkMeasureExistence con meas (mg, measure) 

        

let updateMeasureNames (cConn: Cube) =

    let measInfo = measQuery
    Seq.toList measInfo
    |> List.filter (fun meas -> meas.includeIn)
    |> List.map (fun meas -> updateMeasure cConn meas)

    cConn.MeasureGroups
    |> Seq.cast<MeasureGroup>
    |> Seq.map (fun mg -> mg.Update())