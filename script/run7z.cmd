for /r /d %%x in (*) do (
    pushd "%%x"
    "C:\Program Files\7-Zip\7z.exe" e *.7z -aou -p 
    "C:\Program Files\7-Zip\7z.exe" e *.zip -aou -p 
    "C:\Program Files\7-Zip\7z.exe" e *.rar -aou -p 
    "C:\Program Files\7-Zip\7z.exe" e *.arj -aou -p 
    popd
)