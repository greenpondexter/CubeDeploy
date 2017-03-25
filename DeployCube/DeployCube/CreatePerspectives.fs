module CreatePerspectives

open DataConnections.SqlConnection
open DataConnections.CubeConnection 
open DataConnections.Tables

open System.Data
open System.Data.Linq
open DataConnections.CubeConnection 
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 
open Ops

let persTable = persTable
let persDimAttTable = persDimAttTable
let persDimHierTable = persDimHierTable 
let persMeasTable = persMeasTable
let persCalcMeasTable = persCalcMeasTable

let (<*>) = List.apply 

type Perspective = {
    persId : string
 }

type PersDimAtt = {
    cubeDimId: string;
    cubeDimAttId: string;
    includeIn: bool;
}

type PersDimHier = {
    cubeDimId: string;
    cubeDimHierId: string;
    includeIn: bool;
}

type PersMeas = {
    measGrpId: string;
    measId: string;
    includeIn: bool;
}

type PersCalcMeas = {
    calcMeasId: string;
    includeIn: bool;
}


let persQuery : seq<Perspective> = 
    query {
        for row in persTable do
        select row
    } |> Seq.map (fun row -> {persId = row.PerspectiveID})

let persDimAttQuery (pers:string) : seq<PersDimAtt> = 
    query {
        for row in persDimAttTable do
        where (row.PerspectiveID = pers)
        select row
    } |> Seq.map (fun row -> {cubeDimId = row.CubeDimensionID; cubeDimAttId = row.CubeDimensionAttributeID; includeIn = row.IncludeInPerspective})

let persDimHierQuery (pers:string): seq<PersDimHier> = 
    query {
        for row in persDimHierTable do
        where (row.PerspectiveID = pers)
        select row
    } |> Seq.map (fun row -> {cubeDimId = row.CubeDimensionID; cubeDimHierId = row.CubeDimensionHierarchyID; includeIn = row.IncludeInPerspective})

let persMeasQuery (pers:string): seq<PersMeas> = 
    query {
        for row in persMeasTable do
        where (row.PerspectiveID = pers)
        select row
    } |> Seq.map (fun row -> {measGrpId = row.MeasureGroupID; measId = row.MeasureID; includeIn = row.IncludeInPerspective})

let persCalcMeasQuery (pers:string): seq<PersCalcMeas> = 
    query {
        for row in persCalcMeasTable do
        where (row.PerspectiveID = pers)
        select row
    } |> Seq.map (fun row -> {calcMeasId = row.CalculatedMeasureID; includeIn = row.IncludeInPerspective})



let checkPerspectiveExistence (con: Cube) (pers: string) = 
       
      let persExist = con.Perspectives.ContainsName(pers)
      
      match persExist with
      |true     -> con.Perspectives.Remove(pers) :> obj 
      |false    -> con.Perspectives.Add(pers)    :> obj


let createAttributePerspectives (con: Cube) (pers: string) =
   
    let attPers = persDimAttQuery pers

    let getDim (persId: string) (dim: string * string) =
        let perspective = con.Perspectives.Find(persId)
        if (perspective.Dimensions.Contains((fst dim)) = true) then
            perspective.Dimensions.Find((fst dim)), snd dim
        else 
            perspective.Dimensions.Add((fst dim)), snd dim
   
    attPers
    |> Seq.map (fun persAtt -> getDim pers (persAtt.cubeDimId, persAtt.cubeDimAttId ) ) 
    |> Seq.map (fun dimInfo -> let (dim, att) = dimInfo
                               dim.Attributes.Add(att)
                                )

let createPerspectives (cConn: Cube) =

    let perspectives = persQuery 
    perspectives
    |> Seq.map (fun pers -> (checkPerspectiveExistence cConn pers.persId
                             createAttributePerspectives cConn pers.persId
                                    
                ))
                            
    
     

    0