#requires -version 4
<#
.SYNOPSIS
  Gets all dw objects on server

.DESCRIPTION
  Queries server and retrieves all objects in sql server belonging to all solutions  

.PARAMETER <server>
  <Brief description of parameter input required. Repeat this attribute if required>

.INPUTS
  <Inputs if any, otherwise state None>

.OUTPUTS Log File
  The script log file stored in C:\Windows\Temp\<name>.log

.NOTES
  Version:        1.0
  Author:         Stian Bakke
  Creation Date:  11.02.2016
  Purpose/Change: Initial script development
  
 .LINKS 
  SMO: http://msdn.microsoft.com/en-us/library/ms162169.aspx 

.EXAMPLE
  <Example goes here. Repeat this attribute for more than one example>
  . C:\PowerShell\MyPowerShell\ProcessArchive.ps1  -server "HFALAPTOP" -database "arkReporting" -schema "dbo"  -stgSchema "stg"
  <Example explanation goes here>
#>

param(
    [string]$server=$env:COMPUTERNAME
    )
#---------------------------------------------------------[Initialisations]--------------------------------------------------------

#Set Error Action to Silently Continue
#$ErrorActionPreference = 'SilentlyContinue'
$ErrorActionPreference = 'Stop'


# Reference to SMO 
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO');

#----------------------------------------------------------[Declarations]----------------------------------------------------------

# Configuration data 
# [string] $enabled = 'False'

# Extended Properties Names
# [string] $sourceExtPropName  = 'SourceConnectionString'

# List of objects to return
$dwObjects = @()

# Reference to SMO 
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO');

#-----------------------------------------------------------[Execution]------------------------------------------------------------

# Instantiate SMO
$smoSrv = New-Object Microsoft.SqlServer.Management.Smo.Server $server

#$smoSrv.SetDefaultInitFields([Microsoft.SqlServer.Management.Smo.ExtendedProperty], "ExtendedPropertiesCollection")

# Looping through all databases on server

 
foreach ($db in $smoSrv.Databases)
{ 
    # only add objects if database has solution property
    if($db.ExtendedProperties["Enabled"].Value -eq "True" -and $db.ExtendedProperties["Solution"].Value)
    {   

        # get extended properties from database for including in object

        $dwDbExtProp = @()
        foreach($extProp in $db.ExtendedProperties)
        {
                
            $epDb = [PSCustomObject]@{
                $extProp.Name = $extProp.Value;

            }

            $dwDbExtProp += $epDb

        }


        # loop through all tables in database
        foreach ($tbl in $db.Tables)
        {

            # get all object properties from Extended Properties
            $dwObjectExtProp = @()

            # include options from db
            $dwObjectExtProp += $dwDbExtProp

            foreach($extProp in $tbl.ExtendedProperties)
            {
                
                $epObject = [PSCustomObject]@{
                    $extProp.Name = $extProp.Value;

                }

                $dwObjectExtProp += $epObject

            }
            
            # create PowerShell Custom Object 
            $dwObject = [PSCustomObject]@{
                    "Customer"=$db.ExtendedProperties["Customer"].Value;
                    "Solution"=$db.ExtendedProperties["Solution"].Value;
                    "Server"=$smoSrv.Name; 
                    "Database"=$db.Name; 
                    "Schema"=$tbl.Schema;
                    "Table"=$tbl.Name;
                    "Type"=$db.ExtendedProperties["DatabaseType"].Value;
                    "Enabled"=if($tbl.ExtendedProperties["Enabled"].Value -eq $db.ExtendedProperties["Enabled"].Value) { $db.ExtendedProperties["Enabled"].Value } else { $tbl.ExtendedProperties["Enabled"].Value }

                    "smoObj"=$tbl
                    "Config"=$dwObjectExtProp;
                    } 
            
            # add to object array
            $dwObjects += $dwObject

        }									
    }
}

$dwObjects | Out-GridView

# Dispose of Objects
try {
    $smoSrv.Close()
    $smoSrv.Dispose()
} catch {}