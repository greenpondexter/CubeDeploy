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
   
    Seq.toList attPers
    |> List.map (fun persAtt -> getDim pers (persAtt.cubeDimId, persAtt.cubeDimAttId ) ) 
    |> List.map (fun dimInfo -> let (dim, att) = dimInfo
                                dim.Attributes.Add(att)
                )


let createHierarchyPerspectives (con:Cube) (pers: string) =

    let hierPers = persDimHierQuery pers

    let getHierPers (pdh : PersDimHier)  =
        
        let perspective = con.Perspectives.Find(pers) 
        
        if (perspective.Dimensions.Contains(pdh.cubeDimId) = true) then
            perspective.Dimensions.Find(pdh.cubeDimId), pdh.cubeDimHierId
        else
            perspective.Dimensions.Add(pdh.cubeDimId), pdh.cubeDimHierId

    Seq.toList hierPers
    |> List.filter (fun hier -> hier.includeIn = true)
    |> List.map (fun hier -> getHierPers hier)
    |> List.map (fun persDim -> let (dim, hier) = persDim
                                dim.Hierarchies.Add(hier))    
    
let createMeasurePerspectives (con:Cube) (pers: string) =
    
    let measPers = persMeasQuery pers

    let getMeas (pm : PersMeas) =
        
        let perspective = con.Perspectives.Find(pers) 
        
        if (perspective.MeasureGroups.Contains(pm.measGrpId) = true) then
            perspective.MeasureGroups.Find(pm.measGrpId), pm.measId
        else
            perspective.MeasureGroups.Add(pm.measGrpId), pm.measId

    Seq.toList measPers 
    |> List.filter (fun meas -> meas.includeIn = true)
    |> List.map (fun meas -> getMeas meas)
    |> List.map (fun persMeas -> let (mg,meas) = persMeas
                                 mg.Measures.Add(meas)
                )    

let createCalcMeasurePerspectives (con:Cube) (pers: string) =

    let calcPers = persCalcMeasQuery pers
    
    Seq.toList calcPers
    |> List.filter (fun calc -> calc.includeIn = true)
    |> List.map (fun calc -> con.Perspectives   
                                .Find(pers)
                                .Calculations
                                .Add(calc.calcMeasId) 
                )

let createPerspectives (cConn: Cube) =

    let perspectives = persQuery 
    Seq.toList perspectives
    |> List.map (fun pers -> checkPerspectiveExistence cConn pers.persId
                             createAttributePerspectives cConn pers.persId
                             createHierarchyPerspectives cConn pers.persId
                             createMeasurePerspectives cConn pers.persId
                             createCalcMeasurePerspectives cConn pers.persId
                )
    
    cConn.Update(UpdateOptions.ExpandFull, UpdateMode.Default)