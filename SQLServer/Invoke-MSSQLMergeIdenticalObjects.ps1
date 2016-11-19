param(
    [string]$DestinationServer=$env:COMPUTERNAME,
    [string]$DestinationDatabase, 
    [string]$DestinationSchema="dbo", 
    [string]$DestinationTable, 
    [string]$SourceServer=$DestinationServer,
    [string]$SourceDatabase=$DestinationDatabase,
    [string]$SourceSchema="stg", 
    [string]$SourceObject=$DestinationTable,
    [boolean]$IncludeDeleteClause=$true
    )

# Create SMO Object for accessing Extended Properties
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null;

$SMOserver = New-Object Microsoft.SqlServer.Management.Smo.Server $DestinationServer

$SMOserver.Refresh()

# ColumnCollections
$MergeOnColumns = @()
$MergeUpdateColumnExpression = @()
$MergeUpdateCriteria = @()
$MergeInsertColumnExpression = @()
$MergeValuesColumnExpression = @()
#$MergeDelete ?

function ApplyIsNull ($origin, $columnName, $dataType)
{
    if("text", "ntext", "varchar", "char", "nvarchar", "nchar" -contains $dataType)
        {
            "ISNULL(" + $origin + "." + $columnName + ", '')"
        }
    elseif("tinyint", "smallint", "int", "real", "money", "float", "bit", "decimal", "numeric","smallmoney","bigint" -contains $dataType)
        {
            "ISNULL(" + $origin + "." + $columnName + ", 0)"
        }
    elseif("date", "datetime", "datetime2" -contains $dataType)
        {
            "ISNULL(" + $origin + "." + $columnName + ", '19000101')"
        }
    else         
        {
            $origin + "." + $columnName
        }

}

foreach($column in $SMOserver.Databases[$DestinationDatabase].Tables[$DestinationTable].Columns) # Loop over columns, populating merge-collections
{

    if($column.InPrimaryKey) # Primary key used as ON criteria
    {
        $MergeOnColumns += "T." + $column.Name + " = S." + $column.Name
    }

    if($column.Name -eq "SYS_INSERTED_DATE") # Treat Inserted Date archive table special column
    {
        $MergeInsertColumnExpression += $column.Name
        $MergeValuesColumnExpression += "GETDATE()"
    }
    elseif($column.Name -eq "SYS_UPDATED_DATE") # Treat Updated Date archive table special column
    {
        $MergeUpdateColumnExpression += "T." + $column.Name + " = GETDATE()"
        $MergeInsertColumnExpression += $column.Name
        $MergeValuesColumnExpression += "GETDATE()"
    }
    else
    {
        $MergeUpdateColumnExpression += "T." + $column.Name + " = S." + $column.Name
        $MergeUpdateCriteria += (ApplyIsNull "T" $column.Name $column.DataType.Name) + " <> " + (ApplyIsNull "S" $column.Name $column.DataType.Name)
        $MergeInsertColumnExpression += $column.Name
        $MergeValuesColumnExpression += $column.Name
    }
}

#Build and execute SQL merge command

$Target = $DestinationDatabase + "." + $DestinationSchema + "." + $DestinationTable
$Source = $SourceDatabase + "." + $SourceSchema + "." + $SourceObject

$mergeSQL = @"
USE $DestinationDatabase

DECLARE @MergeStartTime AS DATETIME
DECLARE @C TABLE (act tinyint)

SET @MergeStartTime = GETDATE()

MERGE INTO 
    $($Target) AS T
USING 
    $($Source) AS S
ON 
    $($MergeOnColumns -join(" AND " + "`n`t"))
WHEN MATCHED AND 
    $($MergeUpdateCriteria -join(" OR " + "`n`t"))    
THEN
    UPDATE SET
    $($MergeUpdateColumnExpression -join(", " + "`n`t"))
WHEN NOT MATCHED BY TARGET
THEN
    INSERT
        ( 
        $($MergeInsertColumnExpression -join(", " + "`n`t`t"))
        )
    VALUES
        (
        $($MergeValuesColumnExpression -join(", " + "`n`t`t"))
        )
$(if($IncludeDeleteClause){"WHEN NOT MATCHED BY SOURCE `n THEN `n`t DELETE"})

OUTPUT
    CASE
        WHEN `$action = N'UPDATE' THEN CONVERT(TINYINT, 1)
        WHEN `$action = N'DELETE' THEN CONVERT(TINYINT, 3)
        WHEN `$action = N'INSERT' THEN CONVERT(TINYINT, 4)
    END INTO @C;

INSERT INTO [dbo].[!MERGELOG]
SELECT
	'$DestinationDatabase' AS DatabaseName,
	'$DestinationTable' AS TableName,
	@MergeStartTime AS MergeStartTime,
	GETDATE() AS MergeEndTime,
	[4] AS [InsertCount],
	[1] AS [UpdatedCount],
	[3] AS [DeletedCount]
FROM
	(
	SELECT
		act,
		cnt = COUNT_BIG(*) 
	FROM @C AS c
	GROUP BY
		c.act
	) pvt
	PIVOT (SUM(cnt) FOR act IN ([1], [3], [4])) AS pvt

"@


$SQLConnection = New-Object System.Data.SqlClient.SqlConnection
$SQLConnection.ConnectionString = "Server=$DestinationServer;Database=$DestinationDatabase;Integrated Security=SSPI;Connection Timeout=0"
$SQLConnection.Open()

$SQLCommand = New-Object System.Data.SqlClient.SqlCommand
$SQLCommand.Connection = $SQLConnection

$SQLCommand.CommandTimeout = 0

# execute merge
$SQLCommand.CommandText = $mergeSQL

$ResultExecute = $SQLCommand.ExecuteNonQuery()

#$mergeSQL

$SQLConnection.Close();
$SQLConnection.Dispose();