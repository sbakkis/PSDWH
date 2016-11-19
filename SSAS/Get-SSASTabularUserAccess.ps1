[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices")|Out-Null

$serverAS = New-Object Microsoft.AnalysisServices.Server
$report = @()

$servers = "PRDBISQL02", "TSTBISQL01", "PRDBISQL03", "PRDBISQL03\TABULAR", "TST0030301", "DEVBISQL02"

foreach($server in $servers)
    {
        if($serverAS.Connected)
        {
            $serverAS.Disconnect()
            
        }

        $serverAS.Connect($server)

    Write-Host "Server " $serverAS.Name

    foreach($database in $serverAS.Databases)
    {
        Write-Host "Leser roller for " $database.Name
        # finn alle roller for databasen
        foreach($role in $database.Roles) 
            {
                Write-Host " - " $role.Name " ... "
                # finn evt filter for denne rollen
                $filter = @()

                foreach($dimension in $database.Dimensions)
                    {
                        foreach($permission in ($dimension.DimensionPermissions | Where {$_.Role.Name -eq $role.Name}))
                            {
                                if($permission.AllowedRowsExpression)
                                {
                                    $filter += ($permission.Parent.Name + " : " + $permission.AllowedRowsExpression)
                                }
                            }
                    }

                $filter = $filter -join (", ")  

                #finn alle medlemmer i rollene
                foreach($member in $role.Members)
                {
                    # hvis rolle-medlem er en gruppe, slå opp for å finne bruker
                    $objSID = New-Object System.Security.Principal.SecurityIdentifier ` ($member.Sid) 
                    $objUser = $objSID.Translate( [System.Security.Principal.NTAccount]) 
                
                    $domainName = $objUser.Value.ToString().split('\')[0]
                    $userName = $objUser.Value.ToString().split('\')[1]

                    $adObj = Get-ADObject -Filter {SamAccountName -eq $userName} 

                    # hvis rolle-medlem er en gruppe, finn brukere i gruppen og lag rapportlinjer
                    if($adObj.ObjectClass -eq "group")
                    {
                         $groupMembers = Get-ADGroupMember -Identity $userName

                         foreach($groupMember in $groupMembers)
                         {
                            $reportline = [PSCustomObject]@{"Server"=$serverAS.Name; 
                            "Database"=$database.Name;
                            "Role"=$role.Name;
                            "Group"=$userName;
                            "NTUser"=$groupMember.SamAccountName;
                            "Name"=$groupMember.Name;
                            "Filters"=$filter} 
            
                            $report += $reportline
                         }

                    }
                    # hvis rolle-medlem er bruker, legg ut brukeren som rapportlinje
                    elseif($adObj.ObjectClass -eq "user")
                    {
                        $reportline = [PSCustomObject]@{"Server"=$serverAS.Name; 
                            "Database"=$database.Name;
                            "Role"=$role.Name;
                            "Group"="";
                            "NTUser"=$userName;
                            "Name"=$adObj.Name;
                            "Filters"=$filter} 
            
                            $report += $reportline
                    }

            }
        }
    }
}

$report | Export-CSV -Encoding UTF8 "C:\Profitbase\tabular.csv" -Delimiter ";" -NoTypeInformation

