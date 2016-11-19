param(
    [string]$ServerName,
    [string]$DatabaseName,
    [string]$DatabaseType, 
    [string]$ObjectName, 
    [string]$ObjectType, 
    [string]$Command,
    [string]$Status,
    [string]$Message

    )

#debug

[string] $metadataConnectionString = "Data Source=PRDBISQL02; Initial Catalog=MSBIMetadata; Trusted_Connection=true";

try 
{ 
    # Connect to Metadata Database
    $connMSBIMetadata  =  New-Object System.Data.SqlClient.SqlConnection($metadataConnectionString)

    #Map to Procedure Name
    if($Command -eq "Start")
    {
        $SQLCmdString = "MSBIMetadata.dbo.sp_MSBI_UpdateStart"
    }
    else
    {
        $SQLCmdString = "MSBIMetadata.dbo.sp_MSBI_UpdateEnd"
    }

    $SQLCmd = New-Object System.Data.SqlClient.SqlCommand
    $SQLCmd.CommandType = [System.Data.CommandType]::StoredProcedure

    $SQLCmd.CommandText = $SQLCmdString
    $SQLCmd.CommandTimeout = 120
    $SQLCmd.Connection = $connMSBIMetadata

    $SQLCmd.Parameters.Add("@ServerName",[system.data.SqlDbType]::VarChar) | out-Null
    $SQLCmd.Parameters['@ServerName'].Direction = [system.data.ParameterDirection]::Input
    $SQLCmd.Parameters['@ServerName'].value = $ServerName

    $SQLCmd.Parameters.Add("@DatabaseName",[system.data.SqlDbType]::VarChar) | out-Null
    $SQLCmd.Parameters['@DatabaseName'].Direction = [system.data.ParameterDirection]::Input
    $SQLCmd.Parameters['@DatabaseName'].value = $DatabaseName

    $SQLCmd.Parameters.Add("@DatabaseType",[system.data.SqlDbType]::VarChar) | out-Null
    $SQLCmd.Parameters['@DatabaseType'].Direction = [system.data.ParameterDirection]::Input
    $SQLCmd.Parameters['@DatabaseType'].value = $DatabaseType

    $SQLCmd.Parameters.Add("@ObjectName",[system.data.SqlDbType]::VarChar) | out-Null
    $SQLCmd.Parameters['@ObjectName'].Direction = [system.data.ParameterDirection]::Input
    $SQLCmd.Parameters['@ObjectName'].value = $ObjectName

    $SQLCmd.Parameters.Add("@ObjectType",[system.data.SqlDbType]::VarChar) | out-Null
    $SQLCmd.Parameters['@ObjectType'].Direction = [system.data.ParameterDirection]::Input
    $SQLCmd.Parameters['@ObjectType'].value = $ObjectType

    if($Command -eq "End")
    {
        $SQLCmd.Parameters.Add("@Status",[system.data.SqlDbType]::VarChar) | out-Null
        $SQLCmd.Parameters['@Status'].Direction = [system.data.ParameterDirection]::Input
        $SQLCmd.Parameters['@Status'].value = $Status
    }

    $SQLCmd.Parameters.Add("@StatusMessage",[system.data.SqlDbType]::VarChar) | out-Null
    $SQLCmd.Parameters['@StatusMessage'].Direction = [system.data.ParameterDirection]::Input
    $SQLCmd.Parameters['@StatusMessage'].value = $Message

    [void]$connMSBIMetadata.Open()
    [void]$SQLCmd.ExecuteNonQuery()
        
    [void]$connMSBIMetadata.Close()
    [void]$connMSBIMetadata.Dispose()
        
} 
catch 
{ 
    Write-Output ($_.Exception.Message) 
}

