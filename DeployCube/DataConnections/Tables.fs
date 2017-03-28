module DataConnections.Tables

open DataConnections.SqlConnection

//RemoveUnwantedDimensions
let miParmsTable = db.MI_PARMS

//CheckDatabaseVersion
let getCubeVer = db.ASRPT_FN_GET_CUBE_VER().GetValueOrDefault()

//CreateAggregations
let aggDesignTable = db.ASRPT_AS_METADATA_AGGREGATIONDESIGN
let aggTable = db.ASRPT_AS_METADATA_AGGREGATION

//CreatePerspectives
let persTable = db.ASRPT_AS_METADATA_PERSPECTIVE
let persDimAttTable = db.ASRPT_AS_METADATA_PERSPECTIVEDIMENSIONATTRIBUTE
let persDimHierTable = db.ASRPT_AS_METADATA_PERSPECTIVEDIMENSIONHIERARCHY
let persMeasTable = db.ASRPT_AS_METADATA_PERSPECTIVEMEASURE
let persCalcMeasTable = db.ASRPT_AS_METADATA_PERSPECTIVECALCULATEDMEASURE

//UpdateMeasureNames
let measTable = db.ASRPT_AS_METADATA_MEASURE
