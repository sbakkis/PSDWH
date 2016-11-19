param(
    [string]$Server=$env:COMPUTERNAME,
    [string]$Database, 
    [string]$Schema="dbo", 
    [string]$Table, 
    [string]$TempSchema="stg"
    )

$DebugPreference = "Continue"

# Create SMO Object for accessing Extended Properties
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo")|Out-Null;

$SMOserver = New-Object Microsoft.SqlServer.Management.Smo.Server $Server

#endregion

#region Drop the table if it exists

if ($SMOserver.Databases[$Database].Tables[$Table, $TempSchema])
{
    $SMOserver.Databases[$Database].Tables[$Table, $TempSchema].Drop()
}

#endregion

#region Create the Table
try
{
$newTable = new-object Microsoft.SqlServer.Management.Smo.Table($SMOserver.Databases[$Database], $Table, $TempSchema)

foreach($SourceColumn in $SMOserver.Databases[$Database].Tables[$Table].Columns)
{

        #use same columns as source, except for archive specific ones
        if($SourceColumn.Name -ne "SYS_INSERTED_DATE" -and $SourceColumn.Name -ne "SYS_UPDATED_DATE")
        {
            $column = New-Object Microsoft.SqlServer.Management.Smo.Column($newTable, $SourceColumn.Name, $SourceColumn.DataType)
            $newTable.Columns.Add($column)
        }

}
$newTable.Create();
}

catch
{
    Write-Output $Error
}


#endregion
