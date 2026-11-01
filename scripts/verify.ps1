param([string]$Compiler = '')
$ErrorActionPreference = 'Stop'
if ($Compiler) { $env:CC = $Compiler; $env:CGO_ENABLED = '1' }
function Check([scriptblock]$Command) {
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "Verification failed: $Command" }
}
Check { go mod tidy }
Check { gofmt -w cmd internal }
Check { go vet ./... }
Check { go test ./... }
Check { go test -race ./... }
New-Item -ItemType Directory -Force bin | Out-Null
Check { go build -o bin/cicd.exe ./cmd/cicd }
Check { ./bin/cicd.exe --help }
Check { ./bin/cicd.exe version }
Check { ./bin/cicd.exe validate examples/parallel.yaml }
Check { ./bin/cicd.exe benchmark scheduler --jobs 10000 --workers 32 }
