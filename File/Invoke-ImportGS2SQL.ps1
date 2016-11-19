#
# Invoke-ImportGS2SQL.ps1
#
<#
.SYNOPSIS
  Insert gs2 data into database table (flat)

.DESCRIPTION
  
.PARAMETER <Parameter_Name>
  <Brief description of parameter input required. Repeat this attribute if required>

.INPUTS
   [string]$destinationServer="pfws",$env:COMPUTERNAME,
    [string]$database ="ltl_test", 
	[string]$table="gs2TimeSerieValues",
    [string]$schema="dbo",
	[string]$folderPath="C:\Users\ltl\Documents\Neo_GSM-22-50-16-01.06.13"

.OUTPUTS Log File
  

.NOTES
  Version:        1.0
  Author:        Lars Tore Løvtangen
  Creation Date:  18.10.2016
  Purpose/Change: read from gs2 files into database table
  
 .LINKS 
  SMO: http://msdn.microsoft.com/en-us/library/ms162169.aspx 

.EXAMPLE
  
  
  
#>

param(
    [string]$destinationServer="localhost",$env:COMPUTERNAME,
    [string]$database ="dmReporting_Lyse_Neo", 
	[string]$table="Timeverdi_GS2_Smartly",
    [string]$schema="dbo",
	[string]$folderPath="D:\Lyse Neo Input\"

   # [string]$destinationTable="VolumOgPrognose", 
   # [string]$sourceView="vw_VolumOgPrognose",
   # [string]$truncate="False"
    )


#---------------------------------------------------------[Initialisations]--------------------------------------------------------

#Set Error Action to Silently Continue
#$ErrorActionPreference = 'SilentlyContinue'
$ErrorActionPreference = 'Stop'

#Import PSLogging Module
#Import-Module PostStatusLog

# Reference to SMO 
[void][System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO');

#----------------------------------------------------------[Declarations]----------------------------------------------------------

# Configuration data 
#$file = 'Neo_GSM-22-49-20-01.01.13.gs2'


$files = Get-ChildItem $folderPath -Filter *.gs2 #find all gs2 files in folder

 
foreach ($file in $files){ # loop through all gs2 files in directory
    
	$filecontent = Get-Content  $folderPath\$file #get file content


	#Initialize variables
	$insertColumn = "Filename,"
	$insertValues = "'$file',"
	$insertTSColumn = "Time-series"
	$insertTSValues = ""
	$ts = 0
	$colStartDate = ""
	$colInterval = ""
	$valParts = @()
	$inserts = @()
	$l = 0
	$contentlength = 0


	##-----------------------------------------------------------[Transform filecontent into inserts strings]------------------------------------------------------------
	try {
		$contentlength = $filecontent.Count
		foreach($i in $filecontent){ #read each line in file

			if($i.Length -ne 0){# line contains text
		
				if($i.StartsWith('##Supplier')){ #SupplierId cannot be named Id as in file. Id column allready exists
					$filecontent[$i.ReadCount] = $filecontent[$i.ReadCount].Replace("Id", "SupplierId")
				}
					if($i.StartsWith('##Net-owner')){#Net-ownerId cannot be named Id as in file. Id column allready exists
					$filecontent[$i.ReadCount] = $filecontent[$i.ReadCount].Replace("Id", "Net-ownerId")
				}

				if($i.StartsWith('##Time-series')){#new serie found
					$ts += 1 # increase seq 
					$insertTSColumn = "[Time-series-seq],"
					$insertTSValues = "$ts,"
				}
		 		
				$keyValue = $i.TrimStart('#').Split('=') # read line value

				if($keyValue.Count -eq 2){ #line has key-value pair
				
					$col = $keyValue[0]
					$val = $keyValue[1]

					if($ts -eq 0 ){ #No timeserie has been found yet, insert into master columns
						$insertColumn +=  "["+$col + "],"
						$insertValues +=  "'" + $val + "'," 	
					}
				
					else{ #timeserie found, insert into timeserie columns
					
						if($col -eq "Start"){
							$colStartDate = $val #save start date for later calculations
						}
				
						if($col -eq "Step"){
							$colInterval = $val #save step interval for later calculations
						}
				
						if($col -eq "Value"){ #if value is found, split these values into insert statement, next time an empty row is found

							$valParts = $val.TrimStart('<').TrimEnd('>').Trim().Split(' ')
						}
						else{
							#include into timeserie columns (and values)
							$insertTSColumn +=  "["+$col + "],"
							$insertTSValues +=  "'" + $val + "'," 
						}
					}
				}
			}
		
			#if a null line is read and 
			elseif ($valParts.Count -gt 0){ # values are found and ready to be split into each insert statement
		 
				$nextStartDate = $colStartDate #start date to calculate next step from
				$timeserieStep = "TimeSerieStep"
				$s =  $colInterval   -match '(\d{4})-(\d{2})-(\d{2}).(\d{2}):(\d{2}):(\d{2})' #extract interval from file 
				$nextStartDate = Get-Date -Date $nextStartDate.Replace(".", "T") #first is next initially, convert to date..
				" - $($valParts.Count)"
				foreach($v in $valParts){ #loop through all values
		 			
					#concatinate master columns + timeseries column and Stepcolumn into insert statement, and add to list 
					$cols = $insertColumn + $insertTSColumn + "TimeSerieStep,[Value]"
					$vals = $insertValues + $insertTSValues + "'" + $nextStartDate.ToString("yyyy.MM.dd.HH:mm:ss")  +"'," + $v
					$inserts += [string] "INSERT INTO [$database].[dbo].[$table] VALUES($vals);"

					#increase step by the extracted interval from file into nextstartdate step
					$d = Get-Date  -Date $nextStartDate 
					if ($Matches.Keys.Count -eq 7){
						$d = $d.AddYears([int]$Matches.Item(1))
						$d = $d.AddMonths([int]$Matches.Item(2))
						$d = $d.AddDays([int]$Matches.Item(3))
						$d = $d.AddHours([int]$Matches.Item(4))
						$d = $d.AddMinutes([int]$Matches.Item(5))
						$d = $d.AddSeconds([int]$Matches.Item(6)) 
					}
					$nextStartDate = $d
				} 

				##-----------------------------------------------------------[Execute inserts for Timeserie]------------------------------------------------------------
				try{	

					$SQLConnection = New-Object System.Data.SqlClient.SqlConnection
					$SQLConnection.ConnectionString = "Server=$destinationServer;Database=$database;Integrated Security=SSPI;Connection Timeout=0"
					$SQLConnection.Open()

					[System.Data.SqlClient.SqlCommand] $SQLCommand = $SQLConnection.CreateCommand()
					$SQLCommand.CommandTimeout = 0

					$SQLCommand.CommandText = $inserts #add insert array to sql object 
					$inserts.Count.ToString() + " insertlines found. ready for inserting."
					$Result = $SQLCommand.ExecuteNonQuery() #execute all inserts
					$Result.ToString() + " insertlines inserted"
				}
				catch {
					Error[0] + "Failed" # oops something happend
				}
				finally {    
					#finish up table insertion for this file.
					$SQLConnection.Close();
					$SQLConnection.Dispose();
					$inserts = @()
					"$($i.ReadCount)/$contentlength"
				}
				##-----------------------------------------------------------[Execute inserts for Timeserie]------------------------------------------------------------
				
			}
			
			
		}

		
		##-----------------------------------------------------------[Output inserts to sql file]------------------------------------------------------------
	
		#Set-Content -Path "$folderPath/result_$($file.Replace("gs2","sql"))" -Value $inserts 
 

	}
	catch {
		$Error[0].ToString() # oops something happend
	}
	



}#next file
