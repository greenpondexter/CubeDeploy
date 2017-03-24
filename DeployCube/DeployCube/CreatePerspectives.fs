module CreatePerspectives

open DataConnections.SqlConnection
open System.Data
open System.Data.Linq
open DataConnections.CubeConnection 
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 

//let aggTable = db.ASRPT_AS_METADATA_PERSPECTIVE
//let aggTable = db.ASRPT_AS_METADATA_PERSPECTIVEDIMENSIONATTRIBUTE
//let aggTable = db.ASRPT_AS_METADATA_PERSPECTIVEDIMENSIONHIERARCHY
//let aggTable = db.ASRPT_AS_METADATA_PERSPECTIVEMEASURE
//let aggTable = db.ASRPT_AS_METADATA_PERSPECTIVEMEASURE
//    desId : string;
//    mgId  : string;
//}
//
//let aggDesignQuery : seq<AggDesign> = 
//    query {
//        for row in aggDesignTable do
//        select row
//    } |> Seq.map (fun row -> {desId = row.AggDesignID; mgId = row.MeasureGroupID;})
