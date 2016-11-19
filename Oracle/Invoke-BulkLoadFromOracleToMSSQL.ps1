param(
    [string]$DestinationServer=$env:COMPUTERNAME,
    [string]$DestinationDatabase, 
    [string]$DestinationSchema="stg", 
    [string]$DestinationTable, 
    [string]$SourceConnectionString,
    [string]$SourceSchema,
    [string]$SourceQueryExpression, 
    [string]$SourceWhereClause
    )

# Create SMO Object for accessing Extended Properties
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")|Out-Null;

$SMOserver = New-Object Microsoft.SqlServer.Management.Smo.Server $DestinationServer

#region Create Destination connection and bulk object
$connDst = New-Object System.Data.SqlClient.SqlConnection("Data Source=" + $DestinationServer + ";Integrated Security=True;Initial Catalog=" + $DestinationDatabase + "");
$connDst.Open()
$cmdDst = $connDst.CreateCommand();
$cmdDst.CommandTimeout = 0

# Create source connection for Oracle
$connSrc = New-Object System.Data.OleDb.OleDbConnection($SourceConnectionString)
$connSrc.Open()
$cmdSrc = $connSrc.CreateCommand();
$cmdSrc.CommandTimeout = 0
#endregion

# Set Command for Source Query
if($SourceQueryExpression)
    {
        $cmdSrc.CommandText = $SourceQueryExpression
    }
else
    {
        $cmdSrc.CommandText = "SELECT * FROM " + $SourceSchema + "." + $DestinationTable
    }

if($SourceWhereClause)
    {
        $cmdSrc.CommandText = $cmdSrc.CommandText + " WHERE " + $SourceWhereClause
    }

#region create .net reader and bulk load object. Set properties
[System.Data.OleDb.OleDbDataReader] $dr = $cmdSrc.ExecuteReader()
$bc = new-object ("System.Data.SqlClient.SqlBulkCopy") $connDst

$bc.DestinationTableName = $DestinationSchema + "." + $DestinationTable

$bc.BulkCopyTimeout = 0
$bc.EnableStreaming = 1
$bc.BatchSize = 10000
#endregion

#region do column mapping
foreach($SourceColumn in $SMOserver.Databases[$DestinationDatabase].Tables[$DestinationTable, $DestinationSchema].Columns)
    {
        $bc.ColumnMappings.Add($SourceColumn.Name, $SourceColumn.Name) | Out-Null
    }
#endregion

# do the load
try
{
    $bc.WriteToServer($dr)
}
catch
{            
    $Error
}

#$cmdDst.CommandText = "DROP TABLE " + "stg." + $Table
#$cmdDst.ExecuteScalar() | Out-Null

# Dispose of Objects

$bc.Close()
$bc.Dispose()
 
$dr.Close()
$dr.Dispose()

$connDst.Close();
$connDst.Dispose();
        
$connSrc.Close();
$connSrc.Dispose();

$cmdSrc.Dispose();
$cmdDst.Dispose();