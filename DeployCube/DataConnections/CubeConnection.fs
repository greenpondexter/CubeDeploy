module DataConnections.CubeConnection 

open System 
open System.Linq  
open System.Text  
open Microsoft.AnalysisServices 
open Microsoft.AnalysisServices.Hosting 

    type CubeConfig = string * string * string
    type ServerConnect = Server * CubeConfig
    type DatabaseConnect = Database * CubeConfig 

    let connectToServer (cc : CubeConfig) = 
        let (Server, _, __) = cc
        let ConnStr = "Provider=MSolap;Data Source="+ Server + ""
        let olapServer = new Server()
        try  
            olapServer.Connect(ConnStr)
        with 
            | :? ConnectionException -> printfn "Cannot connect to AS server/cube"
        olapServer, cc

    let connectToDatabase (sc:ServerConnect)  = 
        let (serv, cc) = sc
        let (_, db, __) = cc
        serv.Databases.FindByName(db), cc
        
    let connectToCube (dbc:DatabaseConnect) : Cube =
        let (db, cc) = dbc 
        let (_,__,cube) = cc 
        db.Cubes.GetByName(cube)

    let cubeCount (cc:int) =
        printfn "The number is: %d" cc

    let getCubeConnection = 
        connectToServer
        >> connectToDatabase 
        >> connectToCube
