$files = @(
  @{ Path = "data/departments.csv"; Table = "departments" },
  @{ Path = "data/jobs.csv"; Table = "jobs" },
  @{ Path = "data/hired_employees.csv"; Table = "hired_employees" }
)

foreach ($f in $files) {
  Write-Host "Uploading $($f.Path) to /upload-csv/$($f.Table)"
  $resp = curl.exe -s -o response.json -w "%{http_code}" -F "file=@$($f.Path)" "http://127.0.0.1:8000/upload-csv/$($f.Table)"
  if ($resp -eq "200") {
    Write-Host "OK $($f.Path)"
  } else {
    Write-Host "FAILED $($f.Path) with status $resp"
    Get-Content response.json
    exit 1
  }
}
Write-Host "All files uploaded"
