# wordfeud-cdf
Connect your Wordfeud to CDF

## Prerequisites

Before setting up this extractor, you need to:

1. **Wordfeud API Package**: This script depends on the forked Wordfeud API from [https://github.com/mallpunk/Python-Wordfeud-API](https://github.com/mallpunk/Python-Wordfeud-API). Install the Wordfeud API package:
   ```bash
   pip install git+https://github.com/mallpunk/Python-Wordfeud-API.git
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
   python3 handler.py -c EXTRACTOR_CLIENT_ID -k EXTRACTOR_CLIENT_SECRET -t CDF_AD_TENANT_ID -p CDF_PROJECT -b CDF_CLUSTER_BASE_URL -i True -d DATA_SET_ID -a SECURITY_CATEGORY_ID --board_type BoardNormal --rule_set RuleSetNorwegian
   ```
   
   **For other IDPs:**
   ```bash
   python3 handler.py -c EXTRACTOR_CLIENT_ID -k EXTRACTOR_CLIENT_SECRET --token_url CDF_TOKEN_URL -p CDF_PROJECT -b CDF_CLUSTER_BASE_URL -i True -d DATA_SET_ID -a SECURITY_CATEGORY_ID --board_type BoardNormal --rule_set RuleSetNorwegian
   ```
   
   **Note:** 
   - Credentials are loaded from `credentials.py` file
   - Extraction pipeline is automatically generated as `extractors/wordfeud-{username}` from your credentials
   - You can override the extraction pipeline with `--extraction_pipeline custom-pipeline-id` if needed
   
   **Cleanup Option:** To delete existing time series before creating new ones, add the `--cleanup` flag:
   ```bash
   python3 handler.py -c EXTRACTOR_CLIENT_ID -k EXTRACTOR_CLIENT_SECRET --token_url CDF_TOKEN_URL -p CDF_PROJECT -b CDF_CLUSTER_BASE_URL -i True -d DATA_SET_ID -a SECURITY_CATEGORY_ID --board_type BoardNormal --rule_set RuleSetNorwegian --cleanup
   ```
   
   **Board Configuration Options:**
   - **Board Types**: `BoardNormal` (default), `BoardRandom`
   - **Rule Sets**: `RuleSetNorwegian` (default), `RuleSetAmerican`, `RuleSetDanish`, `RuleSetDutch`, `RuleSetEnglish`, `RuleSetFrench`, `RuleSetSpanish`, `RuleSetSwedish`

   **Available Rule Sets:**
   - `RuleSetAmerican` - American English rules
   - `RuleSetDanish` - Danish rules
   - `RuleSetDutch` - Dutch rules
   - `RuleSetEnglish` - English rules
   - `RuleSetFrench` - French rules
   - `RuleSetNorwegian` - Norwegian rules (default)
   - `RuleSetSpanish` - Spanish rules
   - `RuleSetSwedish` - Swedish rules

   **Board Types:**
   - `BoardNormal` - Standard board layout (default)
   - `BoardRandom` - Randomized board layout
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
    - **Data configuration is optional** - the function will auto-generate the extraction pipeline name from the `wordfeud-user` secret
    - If you want to specify a custom extraction pipeline, set data to:
    ```json
    {
      "extraction-pipeline": "extractors/wordfeud-custom"
    }
    ```
    **Note:** The function automatically uses `extractors/wordfeud-{username}` based on your `wordfeud-user` secret.

## Backfill

If you need to backfill data, you may call the Cognite function for the extractor with the data parameter set to:

```json
{
  "start-time": UNIX_TIMESTAMP_IN_MS
}
```

**Note:** The extraction pipeline is automatically generated from your `wordfeud-user` secret. If you need a custom pipeline, you can specify it explicitly:

```json
{
  "extraction-pipeline": "extractors/wordfeud-custom",
  "start-time": UNIX_TIMESTAMP_IN_MS
}
```

## Command Line Arguments

The extractor supports the following command line arguments:

### Required Arguments
- `-c, --client_id`: CDF OIDC client ID
- `-k, --key`: CDF OIDC client secret
- `-p, --project`: CDF project name

### IDP Configuration (choose one)
- `-t, --tenant_id`: Azure AD tenant ID (for Azure AD)
- `--token_url`: Token URL (for other IDPs)

### Optional Arguments
- `-b, --base_url`: CDF cluster base URL (default: https://api.cognitedata.com)
- `-i, --init`: Initialize time series and extraction pipeline (default: False)
- `--cleanup`: Delete existing time series before creating new ones (requires --init)
- `-d, --dataset`: Dataset ID from CDF
- `-a, --admin_security_category`: Security category ID
- `--extraction_pipeline`: External ID for extraction pipeline (auto-generated from username if not specified)
- `--board_type`: Wordfeud board type - BoardNormal or BoardRandom (default: BoardNormal)
- `--rule_set`: Wordfeud rule set/language (default: RuleSetNorwegian)
- `-s, --start_time`: Begin timestamp in milliseconds (default: one week ago)
- `-e, --end_time`: End timestamp in milliseconds (default: now)

## Data Structure

The extractor will create the following time series in CDF (where USERNAME is your configured username):

- `WORDFEUD/USERNAME/rating` - Wordfeud Rating - USERNAME (your current rating over time)
- `WORDFEUD/USERNAME/games_played` - Wordfeud Games Played - USERNAME (number of games played)
- `WORDFEUD/USERNAME/games_won` - Wordfeud Games Won - USERNAME (number of games won)
- `WORDFEUD/USERNAME/win_rate` - Wordfeud Win Rate - USERNAME (win rate percentage)
- `WORDFEUD/USERNAME/current_streak` - Wordfeud Current Streak - USERNAME (current winning/losing streak)
- `WORDFEUD/USERNAME/best_rating` - Wordfeud Best Rating - USERNAME (best rating achieved)

**Features:**
- **Step Charts**: All time series are configured as step charts (`is_step=True`) for better visualization of discrete changes
- **Game-Based Datapoints**: Datapoints are created only when games are completed, not on every extractor run
- **Accurate Timestamps**: Uses game finish timestamps instead of extractor run timestamps
- **Metadata**: Each datapoint includes metadata about the game (game ID, result, opponent, rating change)
- **Rating Tracking**: Uses the Wordfeud API's per-game rating information for accurate rating changes

**How It Works:**
1. **Extractor runs every 20 minutes** but only creates datapoints when games are completed
2. **Checks for new completed games** since the last datapoint was created
3. **Uses Wordfeud API's rating data** to get the exact rating after each game
4. **Creates datapoints with game metadata** including rating change, opponent, and game result
5. **Uses step chart visualization** to show discrete changes rather than continuous lines

**Datapoint Metadata:**
Each datapoint includes metadata with the following information:
- `game_id`: The Wordfeud game ID
- `result`: Game result (won/lost)
- `opponent`: Opponent's username
- `rating_delta`: Rating change for this game
- `ruleset`: Game rule set used
- `board`: Board type used

**Note:** Wordfeud credentials are stored securely in the CDF function secrets, not in time series data.

## Cleanup and Fresh Start

The extractor includes a cleanup feature to delete existing time series before creating new ones:

### Cleanup Process
1. **Lists existing time series** for the specified username
2. **Shows time series details** (name and external ID)
3. **Requests user confirmation** before deletion
4. **Deletes only the time series** that will be recreated
5. **Handles cases** where no time series exist

### Usage
Add the `--cleanup` flag to the initialization command:
```bash
python3 handler.py -c CLIENT_ID -k CLIENT_SECRET --token_url TOKEN_URL -p PROJECT -b BASE_URL -i True -d DATASET_ID --cleanup
```

### Example Output
```
🧹 Cleanup mode: Checking for existing time series for user 'epistel'...
Found 6 existing time series for user 'epistel':
  - Wordfeud Rating - epistel (WORDFEUD/epistel/rating)
  - Wordfeud Games Played - epistel (WORDFEUD/epistel/games_played)
  - Wordfeud Games Won - epistel (WORDFEUD/epistel/games_won)
  - Wordfeud Win Rate - epistel (WORDFEUD/epistel/win_rate)
  - Wordfeud Current Streak - epistel (WORDFEUD/epistel/current_streak)
  - Wordfeud Best Rating - epistel (WORDFEUD/epistel/best_rating)

❓ Do you want to delete these time series? (yes/no): yes
✅ Successfully deleted existing time series
✅ Cleanup completed successfully
```

## Credential Management

The extractor handles credentials and board configuration differently depending on the execution environment:

- **Local Testing/Initialization**: Credentials are loaded from `credentials.py` file
- **CDF Function Execution**: Credentials are loaded from function secrets:
  - `wordfeud-email`, `wordfeud-pass`, and `wordfeud-user` for credentials
  - `board-type` and `rule-set` for board configuration (optional)

This approach ensures credentials are never stored in CDF time series while still allowing local testing and initialization.

## Extraction Pipeline Management

The extractor automatically handles extraction pipeline naming:

- **Local Development**: Auto-generates `extractors/wordfeud-{username}` from `credentials.py`
- **CDF Function**: Auto-generates `extractors/wordfeud-{username}` from `wordfeud-user` secret
- **Override Option**: You can specify a custom extraction pipeline with `--extraction_pipeline` or in function data

This ensures consistent naming across environments while allowing customization when needed.

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
- If datapoints are not being created, check that games have been completed since the last run
- If rating values seem incorrect, the extractor now uses per-game rating data from the Wordfeud API
- For cleanup issues, ensure you have proper permissions to delete time series in CDF
- If extraction pipeline reporting fails, check that the pipeline exists and the function has proper permissions
- For auto-generated pipeline naming issues, verify that the `wordfeud-user` secret is correctly configured 