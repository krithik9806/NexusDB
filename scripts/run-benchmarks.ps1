# NexusDB Benchmark Suite Runner
$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "             NexusDB Benchmark Suite Runner               " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$env:JAVA_HOME = "C:\Program Files\Android\Android Studio\jbr"
$mavenBin = "$env:USERPROFILE\.local\apache-maven-3.9.9\bin"
$env:PATH = "$env:JAVA_HOME\bin;$mavenBin;$env:PATH"

Write-Host "Running Index vs Table Scan Benchmark..." -ForegroundColor Yellow
mvn test -Dtest=IndexVsSeqScanBenchmarkTest

Write-Host "Running Concurrency and Thread Scaling Benchmark..." -ForegroundColor Yellow
mvn test -Dtest=ClusterBenchmarkRunnerTest

Write-Host "Benchmarks Completed Successfully!" -ForegroundColor Green
