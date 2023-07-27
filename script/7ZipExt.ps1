param([string]$folderPath="C:\Documents and Settings\7810155\Documents\Projects\Python\acc51parser\STMTData\")

Get-ChildItem $folderPath -recurse | %{ 

    if($_.Name -match "^*.`.zip$")
    {

        $NewName = -join ((97..122) | Get-Random -Count 25 | ForEach-Object {[char]$_})

        $parent="$(Split-Path $_.FullName -Parent)" # + '\' + $NewName + '\';    
        write-host "Extracting $($_.FullName) to $parent"

        #$arguments=@("e", "`"$($_.FullName)`"", "-o`"$($parent)\$($NewName)_$($_.BaseName)`" -aou");
        $arguments=@("x", "`"$($_.FullName)`"", "-o`"$($parent)`" -aou");
        $ex = start-process -FilePath "`"C:\Program Files\7-Zip\7z.exe`"" -ArgumentList $arguments -wait -PassThru;

        if( $ex.ExitCode -eq 0)
        {
            write-host "Extraction successful"
        }
    }
}

Get-ChildItem $folderPath -recurse | %{ 

    if($_.Name -match "^*.`.7z$")
    {

        $NewName = -join ((97..122) | Get-Random -Count 25 | ForEach-Object {[char]$_})

        $parent="$(Split-Path $_.FullName -Parent)" + '\' + $NewName + '\';    
        write-host "Extracting $($_.FullName) to $parent"

        #$arguments=@("e", "`"$($_.FullName)`"", "-o`"$($parent)\$($_.BaseName)`"");
        $arguments=@("x", "`"$($_.FullName)`"", "-o`"$($parent)`" -aou");

        $ex = start-process -FilePath "`"C:\Program Files\7-Zip\7z.exe`"" -ArgumentList $arguments -wait -PassThru;

        if( $ex.ExitCode -eq 0)
        {
            write-host "Extraction successful"
        }
    }
}

Get-ChildItem $folderPath -recurse | %{ 

    if($_.Name -match "^*.`.rar$")
    {

        $NewName = -join ((97..122) | Get-Random -Count 25 | ForEach-Object {[char]$_})

        $parent="$(Split-Path $_.FullName -Parent)" + '\' + $NewName + '\';    
        write-host "Extracting $($_.FullName) to $parent"

        #$arguments=@("e", "`"$($_.FullName)`"", "-o`"$($parent)\$($_.BaseName)`"");
        $arguments=@("x", "`"$($_.FullName)`"", "-o`"$($parent)`" -aou");
        $ex = start-process -FilePath "`"C:\Program Files\7-Zip\7z.exe`"" -ArgumentList $arguments -wait -PassThru;

        if( $ex.ExitCode -eq 0)
        {
            write-host "Extraction successful"
        }
    }
}