curl -LO https://github.com/HappyY19/data-analysis/releases/download/v0.1.9/data_analysis
chmod +x ./data_analysis
source ~/.secrets
./data_analysis \
--cxone_access_control_url https://sng.iam.checkmarx.net \
--cxone_server https://sng.ast.checkmarx.net \
--cxone_tenant_name happy \
--cxone_grant_type refresh_token \
--cxone_refresh_token $CXONE_HAPPY_TOKEN \
--include_not_exploitable false \
--range_type CUSTOM \
--date_from 2024-06-01-0-0-0 \
--date_to 2026-02-28-0-0-0
