# wordfeud-cdf
Connect your Wordfeud to CDF

## Prerequisites

Before setting up this extractor, you need to:

1. **Wordfeud API Package**: Install the Wordfeud API package:
   ```bash
   pip install wordfeud-api
   ```
   Or if you have a local development version:
   ```bash
   pip install -e ../Python-Wordfeud-API
   ```

2. **Wordfeud Account**: You will need a Wordfeud account and login credentials.
   - Register at https://wordfeud.com if you don't have an account
   - Note down your email and password for the Wordfeud account

## Setup

1. Clone this repository and navigate to the wordfeud-cdf directory
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `credentials.py` file with your Wordfeud login details:
   ```bash
   cp credentials_template.py credentials.py
   # Edit credentials.py with your Wordfeud email, password, and username
   ```
4. In the Fusion UI, create a new data set and write down the data set ID.
5. In the Fusion UI, create a new security category. Write down the ID of the security category.
6. Create an identity provider application for the extractor:
   
   **For Azure AD:**
   - Create a new Active Directory application in your Azure Active Directory
   - Write down the Client ID and create and write down a Client Secret of this app.
   - We will refer to these as the EXTRACTOR_CLIENT_ID and EXTRACTOR_CLIENT_SECRET.
   - In addition, you should note down the Tenant ID of the Azure Active Directory itself.
   - We will refer to that as CDF_AD_TENANT_ID.
   
   **For other IDPs:**
   - Create an application in your identity provider
   - Write down the Client ID and create and write down a Client Secret of this app.
   - We will refer to these as the EXTRACTOR_CLIENT_ID and EXTRACTOR_CLIENT_SECRET.
   - Note down the token URL for your IDP (e.g., `https://your-idp.com/oauth2/token`)
   - We will refer to that as CDF_TOKEN_URL.

7. Create a group in your identity provider and add the new application to it.
   - Write down the Object ID of this group.

8. Create a group in CDF with the following capabilities:
   - `timeseries:read and write` scoped to the dataset created in step 4. 
   - `extractionpipelines:write` scoped to dataset created in step 4.
   - `extractionruns:write` scoped to dataset created in step 4.
   - `datasets:read` scoped to dataset created in step 4.
   - Set the Source ID of the group to the Object ID found in step 7.

9. Initialize the extractor by running:
   
   **For Azure AD:**
   ```bash
   python3 handler.py -c EXTRACTOR_CLIENT_ID -k EXTRACTOR_CLIENT_SECRET -t CDF_AD_TENANT_ID -p CDF_PROJECT -b CDF_CLUSTER_BASE_URL -i True -d DATA_SET_ID -a SECURITY_CATEGORY_ID --board_type BoardNormal --rule_set RuleSetNorwegian --extraction_pipeline extractors/wordfeud-USERNAME
   ```
   
   **For other IDPs:**
   ```bash
   python3 handler.py -c EXTRACTOR_CLIENT_ID -k EXTRACTOR_CLIENT_SECRET --token_url CDF_TOKEN_URL -p CDF_PROJECT -b CDF_CLUSTER_BASE_URL -i True -d DATA_SET_ID -a SECURITY_CATEGORY_ID --board_type BoardNormal --rule_set RuleSetNorwegian --extraction_pipeline extractors/wordfeud-USERNAME
   ```
   
   **Note:** Credentials are now loaded from `credentials.py` file. Replace `USERNAME` with your actual username.
   
   **Board Configuration Options:**
   - **Board Types**: `BoardNormal` (default), `BoardRandom`
   - **Rule Sets**: `RuleSetNorwegian` (default), `RuleSetAmerican`, `RuleSetDanish`, `RuleSetDutch`, `RuleSetEnglish`, `RuleSetFrench`, `RuleSetSpanish`, `RuleSetSwedish`
10. Create a zip file for CDF function deployment:
    ```bash
    # The zip file includes the Wordfeud API code directly
    # This is handled automatically by the project setup
    # The handler.zip file is ready for deployment
    ```
11. Create a new CDF function in the Functions section of the Fusion UI.
    Add the following secrets:
    - `wordfeud-email` with value WORDFEUD_EMAIL from step 2.
    - `wordfeud-pass` with value WORDFEUD_PASSWORD from step 2.
    - `wordfeud-user` with value USERNAME from step 2.
    - `board-type` with value `BoardNormal` or `BoardRandom` (optional, defaults to BoardNormal)
    - `rule-set` with value from the rule set options above (optional, defaults to RuleSetNorwegian)
12. Set up a schedule for the function.
    - Set cron expression to every 20 minutes `*/20 * * * *`
    - Set Client ID and Client Secret to EXTRACTOR_CLIENT_ID and EXTRACTOR_CLIENT_SECRET from step 6
    - Set data to:
    ```json
    {
      "extraction-pipeline": "extractors/wordfeud-USERNAME"
    }
    ```
    **Note:** Replace `USERNAME` with your actual username (e.g., "your-username").

## Backfill

If you need to backfill data, you may call the Cognite function for the extractor with the data parameter set to:

```json
{
  "extraction-pipeline": "extractors/wordfeud-USERNAME",
  "start-time": UNIX_TIMESTAMP_IN_MS
}
```

**Note:** Replace `USERNAME` with your actual username (e.g., "your-username").

## Data Structure

The extractor will create the following time series in CDF (where USERNAME is your configured username):

- `WORDFEUD/USERNAME/rating` - Wordfeud Rating - USERNAME (your current rating over time)
- `WORDFEUD/USERNAME/games_played` - Wordfeud Games Played - USERNAME (number of games played)
- `WORDFEUD/USERNAME/games_won` - Wordfeud Games Won - USERNAME (number of games won)
- `WORDFEUD/USERNAME/win_rate` - Wordfeud Win Rate - USERNAME (win rate percentage)
- `WORDFEUD/USERNAME/current_streak` - Wordfeud Current Streak - USERNAME (current winning/losing streak)
- `WORDFEUD/USERNAME/best_rating` - Wordfeud Best Rating - USERNAME (best rating achieved)

**Note:** Wordfeud credentials are stored securely in the CDF function secrets, not in time series data.

## Credential Management

The extractor handles credentials and board configuration differently depending on the execution environment:

- **Local Testing/Initialization**: Credentials are loaded from `credentials.py` file
- **CDF Function Execution**: Credentials are loaded from function secrets:
  - `wordfeud-email`, `wordfeud-pass`, and `wordfeud-user` for credentials
  - `board-type` and `rule-set` for board configuration (optional)

This approach ensures credentials are never stored in CDF time series while still allowing local testing and initialization.

## Deployment Package

The `handler.zip` file includes all necessary dependencies:

- `handler.py` - Main extractor script
- `requirements.txt` - CDF SDK and other dependencies
- `wordfeud_api/` - Embedded Wordfeud API code (not published to PyPI)

**Note:** `credentials.py` is NOT included in the deployment package. Credentials are loaded from CDF function secrets in production.

## Identity Provider Configuration

The extractor supports different identity provider (IDP) configurations:

### Azure AD Configuration
- Use `--tenant_id` parameter with your Azure AD tenant ID
- Token URL is automatically constructed as `https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token`

### Other IDP Configuration
- Use `--token_url` parameter with your IDP's token endpoint
- Example: `--token_url https://your-idp.com/oauth2/token`
- Either `--tenant_id` or `--token_url` must be provided

## Troubleshooting

- If you encounter authentication issues, check that your Wordfeud credentials are correct
- If the extractor fails to connect to CDF, verify your identity provider application configuration
- For Azure AD: Check that your tenant ID is correct and the application has proper permissions
- For other IDPs: Verify that your token URL is correct and the application is properly configured
- For rate limiting issues, the extractor includes built-in retry logic with exponential backoff
- If the Wordfeud API package is not found, ensure it's properly installed: `pip install wordfeud-api` 