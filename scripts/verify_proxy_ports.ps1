# Verify each Clash listener port exits through a different IP.
# Usage: pwsh scripts/verify_proxy_ports.ps1

$ports = @(
    @{ port = 7901; expected = "Hong Kong" },
    @{ port = 7902; expected = "Hong Kong" },
    @{ port = 7903; expected = "Hong Kong" },
    @{ port = 7904; expected = "Hong Kong" },
    @{ port = 7905; expected = "Hong Kong" },
    @{ port = 7906; expected = "Japan" },
    @{ port = 7907; expected = "Japan" },
    @{ port = 7908; expected = "Japan" },
    @{ port = 7909; expected = "Japan" },
    @{ port = 7910; expected = "Japan" }
)

$seen_ips = @{}
$fail = $false

foreach ($p in $ports) {
    $port = $p.port
    $expected = $p.expected
    try {
        $json = curl.exe -s -x "http://127.0.0.1:$port" --max-time 10 "https://ipinfo.io/json"
        if (-not $json) {
            Write-Host "port $port : NO RESPONSE (check Clash is running and listener is configured)" -ForegroundColor Red
            $fail = $true
            continue
        }
        $obj = $json | ConvertFrom-Json
        $ip = $obj.ip
        $country = $obj.country
        $city = $obj.city
        $region_ok = $false
        if ($expected -eq "Hong Kong" -and $country -eq "HK") { $region_ok = $true }
        if ($expected -eq "Japan" -and $country -eq "JP") { $region_ok = $true }

        $dup_msg = ""
        if ($seen_ips.ContainsKey($ip)) {
            $dup_msg = " [DUPLICATE of port $($seen_ips[$ip])]"
            $fail = $true
        } else {
            $seen_ips[$ip] = $port
        }

        $region_msg = if ($region_ok) { "OK" } else { "WRONG REGION (wanted $expected)" }
        $color = if ($region_ok -and -not $dup_msg) { "Green" } else { "Yellow" }
        Write-Host "port $port : ip=$ip  country=$country  city=$city  region=$region_msg$dup_msg" -ForegroundColor $color
        if (-not $region_ok) { $fail = $true }
    } catch {
        Write-Host "port $port : ERROR $($_.Exception.Message)" -ForegroundColor Red
        $fail = $true
    }
}

Write-Host ""
if ($fail) {
    Write-Host "FAILED — at least one port is wrong region, duplicate IP, or unreachable." -ForegroundColor Red
    exit 1
} else {
    Write-Host "All 10 ports healthy: distinct IPs, correct regions." -ForegroundColor Green
}
