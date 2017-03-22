module SqlConnection

open System.Data
open System.Data.Linq
open Microsoft.FSharp.Data.TypeProviders
open Microsoft.FSharp.Linq


type dbSchema = SqlDataConnection<LocalSchemaFile = "MI_105_HCC17.dbml", ForceUpdate = false, ConnectionString = @"Data Source=topsdev02;Initial Catalog=MI_105_ETG;Integrated Security=SSPI;">
let db = dbSchema.GetDataContext()


