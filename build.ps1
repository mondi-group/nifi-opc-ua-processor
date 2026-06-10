Write-Host "Fuehre 'mvn clean install' aus..." -ForegroundColor Cyan
mvn clean install -DskipTests

if ($LASTEXITCODE -eq 0) {
    Write-Host "Build erfolgreich!" -ForegroundColor Green
} else {
    Write-Host "Maven Build fehlgeschlagen. Abbruch." -ForegroundColor Red
}