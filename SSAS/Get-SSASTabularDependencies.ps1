[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices")|Out-Null

$MSBIMetadataConn = New-Object System.Data.SqlClient.SqlConnection("Data Source=TSTBISQL01; Initial Catalog=MSBIMetadata; Trusted_Connection=true")

$serverAS = New-Object Microsoft.AnalysisServices.Server
$dependencies = @()

$servers = "DEVBISQL02", "TST0030301"

foreach($server in $servers)
    {
        if($serverAS.Connected)
        {
            $serverAS.Disconnect()
        }

        $serverAS.Connect($server)

    foreach($databaseAS in $serverAS.Databases)
    {
        Write-Host "Leser tabeller for " $databaseAS.Name

        foreach($cube in $databaseAS.Cubes)
        {   
            foreach($measureGroup in $cube.MeasureGroups)
            {
                foreach($partition in $measureGroup.Partitions)
                {

                    foreach($string in $partition.Source.Parent.DataSource.ConnectionString -split ";")
                    {
                        if(($string -split "=") -eq "Data Source")
                            {
                                $dataSource = ($string -split "=")[1]

                                if($dataSource -eq "localhost" -or $dataSource -eq ".")
                                    {
                                        $dataSource = $serverAS.ParentServer
                                    }
                            }
                        elseif(($string -split "=") -eq "Initial Catalog")
                            {
                                $initialCatalog = ($string -split "=")[1]
                            }
                    }

                    if($database.Name.Length -le 50)
                    {
                        $reportline = [PSCustomObject]@{
                            "Server"=$serverAS.Name; 
                            "Database"=$databaseAS.Name;
                            "Table"=$measureGroup.Name;
                            "Partition"=$partition.Name;
                            "SourceServer"= $dataSource
                            "SourceDatabase"= $initialCatalog
                            "Query"=$partition.Source.QueryDefinition
                            } 
                    
                        if($reportline.Query -ne "")
                        {
                            $dependencies += $reportline
                        }
                    }
                }
            }
        }
        
    }
}

#flush object to SQL Server

$MSBIMetadataConn.Open()
$dbwrite = $MSBIMetadataConn.CreateCommand()

#get timestamp
$CurrentDateTime = get-date
#insert the data

$dependencies | Out-GridView

#foreach($row in $dependencies)
#{
#    $dbwrite.CommandText = @"
#        INSERT INTO tmp.stgSSASContentDependencies (SSASServer, SSASDatabase, SSASTable, SSASTablePartition, SourceServer, SourceDatabase, SourceQuery, InsertedDate) 
#        VALUES ('$($row.Server)','$($row.Database)','$($row.Table)', '$($row.Partition)', '$($row.SourceServer)', '$($row.SourceDatabase)', '$($row.Query -replace "'", "''")', '$($CurrentDateTime)')
#"@
#    $dbwrite.ExecuteNonQuery() | Out-Null

    #$dbwrite.CommandText

#    Write-Host "Test"

#}

#close the database session
$MSBIMetadataConn.Close()

#$dependencies | Out-GridView

#$report | Export-CSV -Encoding UTF8 "C:\Profitbase\tabular.csv" -Delimiter ";" -NoTypeInformation