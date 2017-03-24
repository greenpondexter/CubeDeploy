module DataConnections.Tables

open DataConnections.SqlConnection

let miParmsTable = db.MI_PARMS
let getCubeVer = db.ASRPT_FN_GET_CUBE_VER().GetValueOrDefault()
let aggDesignTable = db.ASRPT_AS_METADATA_AGGREGATIONDESIGN
let aggTable = db.ASRPT_AS_METADATA_AGGREGATION
