module CreateAggregations

open DataConnections.SqlConnection
open DataConnections.Tables
open System.Data
open System.Data.Linq
open DataConnections.CubeConnection
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 

let aggDesignTable = aggDesignTable  
let aggTable = aggTable 

type AggDesign = {
    desId : string;
    mgId  : string;
}

let aggDesignQuery : seq<AggDesign> = 
    query {
        for row in aggDesignTable do
        select row
    } 
    |> Seq.map (fun row -> {desId = row.AggDesignID; mgId = row.MeasureGroupID;})

let aggIdQuery (id:string) = 
    query {
        for row in aggTable do
        where (row.AggDesignID = id)
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
    let aggDesign = cConn.MeasureGroups.Find(mg)
                                   .AggregationDesigns
                                   .GetByName(des)
    
    let aggIds = aggIdQuery des  
    for aggId in aggIds do
        match aggDesign.Aggregations.Find(aggId) with
        |null        -> (aggDesign.Aggregations.Add(aggId)) :> obj
        |Aggregation -> aggDesign.Dimensions.Clear() :> obj

        let aggCubeDims = aggCubeDimIdQuery aggId
        for pair in aggCubeDims do
            let mutable aggDim = aggDesign.Dimensions.Find(fst pair)
            match aggDim with
            |null -> Some(aggDim <- aggDesign.Dimensions.Add(fst pair))
            |_    -> None  
            try 
                aggDim.Attributes.Add(snd pair) :> obj
            with | :? System.InvalidOperationException -> printfn "Attribute %s is allready associated with Dim %s" (snd pair) (fst pair) :> obj

    aggDesign.Update(UpdateOptions.ExpandFull, UpdateMode.Default)

let createAggregations (cConn: Cube) =
    let aggDesigns = aggDesignQuery
    Seq.toList aggDesigns
    |> List.map (fun (aggInfo:AggDesign) -> updateAgg aggInfo cConn) 

    0

    