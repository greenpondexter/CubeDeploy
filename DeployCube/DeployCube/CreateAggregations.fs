module CreateAggregations

open SqlConnection
open System.Data
open System.Data.Linq
open CubeConnection 
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 

let aggDesignTable = db.ASRPT_AS_METADATA_AGGREGATIONDESIGN
let aggTable = db.ASRPT_AS_METADATA_AGGREGATION

type AggDesign = {
    desId : string;
    mgId  : string;
}

let aggDesignQuery = 
    query {
        for row in aggDesignTable do
        select row
    } 
    |> Seq.map (fun row -> {desId = row.AggDesignID; mgId = row.MeasureGroupID;})

let aggIdQuery (id:string) = 
    query {
        for row in aggTable do
        where (row.AggID = id)
        select row.AggID 
    }
    |> Seq.map (fun row -> row)

let aggCubeDimIdQuery (id:string) =
    query {
        for row in aggTable do
        where (row.AggID = id)
        select (row.AggCubeDimensionID, row.AggCubeDimensionAttributeID)
    }
    |> Seq.map (fun (acd, acdi) -> acd, acdi)


let updateAgg (aggInfo:AggDesign) (cConn: Cube) =
    
    let {desId = des; mgId = mg;} = aggInfo
    let aggDesign = cConn.MeasureGroups.GetByName(mg)
                                   .AggregationDesigns
                                   .GetByName(des)
    
    let aggIds = aggIdQuery des  
    for aggId in aggIds do
        match aggDesign.Aggregations.GetByName(aggId) with
        |null        -> (aggDesign.Aggregations.Add(aggId)) :> obj
        |Aggregation -> aggDesign.Dimensions.Clear() :> obj

        let aggCubeDims = aggCubeDimIdQuery aggId
        for pair in aggCubeDims do
            let aggDim = aggDesign.Dimensions.Find(fst pair)
            match aggDim with
            |null -> Some(aggDesign.Dimensions.Add(fst pair))
            |_    -> None  
            aggDim.Attributes.Add(snd pair)
         
    aggDesign.Update(UpdateOptions.ExpandFull, UpdateMode.Default)

let aggDesignLooper (cConn: Cube) =
    let aggDesigns = aggDesignQuery
    aggDesigns
    |> Seq.map (fun aggInfo -> updateAgg aggInfo cConn) 


    