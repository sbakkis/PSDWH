$ServerName = "PRDBISQL03"

[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | out-null

$ServerSMO = new-object ("Microsoft.SqlServer.Management.Smo.Server") $ServerName 

#$archiveTables = C:\Profitbase\PowerShell\Get-PBDWObject.ps1 

#$archiveTables | Where-Object { $_.Table -eq "FAKTURA" -and $_.Type -eq "Archive"} | Out-GridView

function Get-ETLWhereClause
{
    Param(
 
        [parameter(position=0)]
        $smoTable

        )

    try 
    {

        if("text", "ntext", "varchar", "char", "nvarchar", "nchar" -contains $dataType)
            {
                "ISNULL($origin.$columnName,'')"
            }
        elseif("tinyint", "smallint", "int", "real", "money", "float", "bit", "decimal", "numeric","smallmoney","bigint" -contains $dataType)
            {
                "ISNULL($origin.$columnName, 0)"
            }
        elseif("date", "datetime", "datetime2" -contains $dataType)
            {
                "ISNULL($origin.$columnName,'19000101')"
            }
        else         
            {
                "$origin.$columnName"
            }

    } 
    catch 
    {    
        Write-Error $Error[0]
    }
 
}

function Get-ETLSourceQuery
{
    param(
        [Parameter(Position=0, Mandatory=$true)] 
        [string]$sqlserver
       )

    try 
    {
        $sqlconn = new-object ("Microsoft.SqlServer.Management.Common.ServerConnection") $sqlserver
        $sqlconn.Connect()
        Write-Output $sqlconn
    } 
    catch 
    {    
        Write-Error $Error[0]
    }
 
}

function Get-OracleConnection
{
    param(
        [Parameter(Position=0, Mandatory=$true)] 
        [string]$OracleConnectionString
       )

    try 
    {
        $sqlconn = new-object ("Microsoft.SqlServer.Management.Common.ServerConnection") $sqlserver
        $sqlconn.Connect()
        Write-Output $sqlconn
    } 
    catch 
    {    
        Write-Error $Error[0]
    }
 
}

function Get-SqlCommandObject
{
    param(
        [Parameter(Position=0, Mandatory=$true)] 
        [string]$SqlServerName,

        [Parameter(Mandatory = $true)]
        [string]$DatabaseName
       )

    try 
    {
        $ConnectionString = "Data Source = $SqlServerName; Initial Catalog = $DatabaseName; trusted_connection = true;"
        $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
        $SqlConnection.Open()

        $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
        $SqlCmd.Connection = $SqlConnection
        $SqlCmd.CommandTimeout = 0
    } 
    catch 
    {    
        Write-Error $Error[0]
    }

    return $SqlCmd
 
}

function Update-IncrementalValues
{
    Param(
 
        [parameter(position=0)]
        $smoTable

        )

    try 
    {
        # first get command to do database queries
        if(!$sqlCmd)
        {
            $sqlCmd = Get-SqlCommandObject -SqlServerName $smoTable.Parent.Parent.Name -DatabaseName $smoTable.Parent.Name

        }
        # check
        If ($smoTable.ExtendedProperties["IncrementalColumn"])
        {
                $sqlCmd.CommandText = "SELECT ISNULL(MAX(" + $smoTable.ExtendedProperties["IncrementalColumn"].Value + "), 0) FROM " + $smoTable.Schema + "." + $smoTable.Name
                
                $newIncrementalValue = $sqlCmd.ExecuteScalar()
                $smoTable.ExtendedProperties["IncrementalValue"].Value = $newIncrementalValue

                $smoTable.Alter()
        }

    } 
    catch 
    {    
        Write-Error $Error[0]
    }
 
}

function Remove-SqlTable 
{
    param (
        [Parameter(Mandatory = $true)]
        [string]$SqlServerName,

        [Parameter(Mandatory = $true)]
        [string]$DatabaseName,

        [Parameter(Mandatory = $true)]
        [string]$SchemaName,

        [Parameter(Mandatory = $true)]
        [string]$TableName
    )


    $ConnectionString = "Data Source = $SqlServerName; Initial Catalog = $DatabaseName; trusted_connection = true;"
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)

    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection

    $SqlCmd.CommandText = "DROP TABLE $SchemaName.$TableName;"

    try {
        $SqlConnection.Open()
        $SqlCmd.ExecuteNonQuery() | Out-Null
    }
    finally {
        $SqlCmd.Dispose()
        $SqlConnection.Dispose()
    }

    Write-Verbose "Successfully dropped $SchemaName.$TableName in $DatabaseName on $SqlServerName"
}

workflow Invoke-PSDWRefreshArchiveParallell
{
    Param($NumberofIterations)
    "======================================================="
    $array = 1..$NumberofIterations
    $Uri = "http://www.bbc.com."
    
    function DoRequest($i,$Uri){
        "$i starting";$response = Invoke-WebRequest -Uri $Uri;"$i ending"
    }

    "Serial"
    "======"
    $startTime = get-date
    foreach ($i in $array) {DoRequest $i $Uri}
    $serialElapsedTime = "elapsed time (serial foreach loop): " + ((get-date) - $startTime).TotalSeconds
    #versus
    "======================================================="
    "Parallel"
    "========"
    $startTime = get-date
    foreach -parallel ($i in $array) {DoRequest $i $Uri}
    $parallelElapsedTime = "elapsed time (parallel foreach loop): " + ((get-date) - $startTime).TotalSeconds
    $serialElapsedTime
    $parallelElapsedTime
    "======================================================="
}

#$totalStartDateTime = (Get-Date).DateTime


    foreach($db in $ServerSMO.Databases | where {$_.ExtendedProperties["Enabled"].Value -eq "True"}) #$_.Name -like "arkReporting_Lyse" -and 
    {
        #Write-Output "Database :" $db.Name
        #$_.Name -like "W_PARTY_ORG_D" -and 
        foreach($tbl in $db.Tables | where {$_.Schema -eq "dbo" -and $_.IsSystemObject -eq $false -and $_.Name -notlike "!*" -and $_.ExtendedProperties["Enabled"].Value -eq "True"}) 
        {
               
            $SourceWhereClause = @()

            # see if this table has filter
            if($tbl.ExtendedProperties["IncludeFilter"].Value)
            {
                $SourceWhereClause += $tbl.ExtendedProperties["IncludeFilter"].Value
            }

            # see if this table has criteria for load
            if($tbl.ExtendedProperties["IncrementalColumn"].Value)
            {
                Update-IncrementalValues $tbl

                $tbl.ExtendedProperties.Refresh()

                $SourceWhereClause += $tbl.ExtendedProperties["IncrementalColumn"].Value + " >= " + $tbl.ExtendedProperties["IncrementalValue"].Value
            }
                $Error.Clear()
        

            if($tbl.ExtendedProperties["SourceSchema"].Value -eq "")
            {
                $querySchema = $db.ExtendedProperties["SourceSchema"].Value
            }
            else
            {
                $querySchema = $tbl.ExtendedProperties["SourceSchema"].Value
            }

            try
            {
            
                # Inform MSBI that we are processing this object
                C:\Profitbase\PowerShell\Invoke-MSBIProcessingCommand.ps1 $env:COMPUTERNAME $db.Name "SQL" $tbl.Name "Table" "Start" 0 "Start Processing..."
                
                # Inform Focus Monitoring that we are processing this object 
                C:\Profitbase\PowerShell\Invoke-UpdateCloudObjectStatus.ps1 -type Table -schema $tbl.Schema -table $tbl.Name -statusText Processing | Out-Null

                #Write-Host "Tabell : " $($db.Name + '.dbo.' + $tbl.Name) -Foreground "green"
            
                C:\Profitbase\PowerShell\Create-TempTable.ps1 -Server $ServerName -Database $db.Name -Schema $tbl.Schema -Table $tbl.Name -TempSchema "stg"
                #Write-Host " - temp table created"

                $tableLoadStartDateTime = (Get-Date).DateTime

                C:\Profitbase\PowerShell\Invoke-BulkLoadFromOracle.ps1 -DestinationServer $ServerName -DestinationDatabase $db.Name -DestinationSchema "stg" -DestinationTable $tbl.Name -SourceConnectionString $db.ExtendedProperties["SourceConnectionString"].Value -SourceSchema $querySchema -SourceQueryExpression $tbl.ExtendedProperties["QueryExpression"].Value -SourceWhereClause $($SourceWhereClause -join(" AND "))
            
                $tableLoadEndDateTime = (Get-Date).DateTime
            
                #Write-Host " - data loaded : " $(New-Timespan -Start $tableLoadStartDateTime -End $tableLoadEndDateTime)

                $tableMergeStartDateTime = (Get-Date).DateTime

                C:\Profitbase\PowerShell\Merge-IdenticalTables.ps1 -DestinationServer $ServerName -DestinationDatabase $db.Name -DestinationSchema $tbl.Schema -DestinationTable $tbl.Name -SourceSchema "stg" -IncludeDeleteClause $(if ($tbl.ExtendedProperties["IncrementalColumn"].Value -eq "") { $true } else { $false })
            
                $tableMergeEndDateTime = (Get-Date).DateTime

                #Write-Host " - merge to archive complete : " $(New-Timespan -Start $tableMergeStartDateTime -End $tableMergeEndDateTime)

                Remove-SqlTable -SqlServerName $ServerName -DatabaseName $db.Name -SchemaName "stg" -TableName $tbl.Name
                #Write-Host " - temp table dropped" 

                Write-Output $($db.Name + '.' + $tbl.Schema + '.' + $tbl.Name) "Load time : " $(New-Timespan -Start $tableLoadStartDateTime -End $tableLoadEndDateTime).ToString() "Merge time : " $(New-Timespan -Start $tableMergeStartDateTime -End $tableMergeEndDateTime).ToString()

                # Inform MSBI that we have finished ok
                C:\Profitbase\PowerShell\Invoke-MSBIProcessingCommand.ps1 $env:COMPUTERNAME $db.Name "SQL" $tbl.Name "Table" "End" 2 "Finished OK"

                # Inform Focus Monitoring that we have finished ok
                C:\Profitbase\PowerShell\Invoke-UpdateCloudObjectStatus.ps1 -type Table -schema $tbl.Schema -table $tbl.Name -statusText Completed | Out-Null
            }
            catch
            {

                C:\Profitbase\PowerShell\Invoke-MSBIProcessingCommand.ps1 $env:COMPUTERNAME $db.Name "SQL" $tbl.Name "Table" "End" -2 ($Error.Item(0) -split '\n')[0]

                C:\Profitbase\PowerShell\Invoke-UpdateCloudObjectStatus.ps1 -type Table -schema $tbl.Schema -table $tbl.Name -statusText Error | Out-Null
            }
       
        }

    }


#$totalEndDateTime = (Get-Date).DateTime

Write-Output " "
Write-Output "Time elapsed :" $(New-Timespan -Start $totalStartDateTime -End $totalEndDateTime).ToString()
