# Data Analysis

## How to run
1. download the binary from [Releases](https://github.com/HappyY19/data-analysis/releases).
2. chmod +x ./data_analysis
3. run the cli: 
```commandline
data_analysis --cxone_access_control_url https://eu.iam.checkmarx.net \
--cxone_server https://eu.ast.checkmarx.net \
--cxone_tenant_name <tenant_name> \
--cxone_grant_type refresh_token \
--cxone_refresh_token <api_key> \
--include_not_exploitable false \
--range_type CUSTOM \
--date_from 2023-06-01-0-0-0 \
--date_to 2026-02-31-0-0-0
```
 
 