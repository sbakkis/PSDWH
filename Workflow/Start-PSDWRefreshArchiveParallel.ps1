Workflow Start-PSDWRefreshArchiveParallel
{
    # InlineScript to call script at UNC path - Get a list of SQL Servers
    $server_list = InlineScript {
        $AllServers = \\share\scripts\Get-SQLServerList.ps1
        $exclusions = @()
        $server_list = $AllServers | ? { -NOT( $exclusions -contains $_ ) }
        $server_list
    }

    # Loop through ($server_list count) servers at a time
    ForEach -Parallel -ThrottleLimit ($server_list.Count) ( $server in $server_list )
    {
        # Write the output that we are starting this server
        $starting = New-Object -Type PSObject -Prop @{
            Server = $Server
            Database = "N/A"
            Operation = "Starting work on $Server"
            LogSize = "N/A"
            Time = (Get-date -uFormat '%Y-%m-%d %r')
            RebuildQueue = "N/A"
        }
        $starting

        # Get a list of databases on the server
        $databases = InlineScript {
            $server = $using:server
            Write-Verbose "Checking $server"
            [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") |out-null
            $sql = New-Object ('Microsoft.SqlServer.Management.Smo.Server') $server
            $databases = @($sql.databases | ? { $_.Id -gt 5 } | select -expand Name)
            $databases
        }

        # Run up to 10 databases per server at a time
        ForEach -Parallel -ThrottleLimit 10 ( $database in $databases ) {
            # Catch the output of the InlineScript which runs the script logic
            $output = InlineScript {
                # Preserve the global script variables into the InlineScript
                $server = $using:server
                $database = $using:database

                #Load the SMO Assemblies inside the InlineScript
                [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") |out-null          # Base SMO functionality
                [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SmoExtended") |out-null  # Needed for backups

                $sql = New-Object ('Microsoft.SqlServer.Management.Smo.Server') $server

                # Starting rebuild
                New-Object -Type PSObject -Prop @{
                    Server = $Server
                    Database = $database
                    Operation = "Starting RebuildIndexes"
                    LogSize = $InitialLogSize
                    InitialSize = $InitialLogSize
                    Time = (Get-date -uFormat '%Y-%m-%d %r')
                    RebuildQueue = $indexesBefore
                }
                #
                #   Rebuild Index logic
                #

                # Finished rebuild
                New-Object -Type PSObject -Prop @{
                    Server = $Server
                    Database = $database
                    Operation = "Finished RebuildIndexes"
                    LogSize = $InitialLogSize
                    InitialSize = $InitialLogSize
                    Time = (Get-date -uFormat '%Y-%m-%d %r')
                    RebuildQueue = $indexesAfter
                }
            }
            # Output the result of our InlineScript so it can be captured
            $output
        }
        $finished = New-Object -Type PSObject -Prop @{
            Server = $Server
            Database = "N/A"
            Operation = "Finished work on $Server"
            LogSize = "N/A"
            Time = (Get-date -uFormat '%Y-%m-%d %r')
            RebuildQueue = "N/A"
        }
        $finished
    }
}