for /r /d %%x in (*) do (
    pushd "%%x"
    dir
    "C:\Program Files\7-Zip\7z.exe" e * -aou -p 
    popd
)