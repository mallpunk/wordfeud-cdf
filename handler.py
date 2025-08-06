#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Send Wordfeud rating data to CDF
"""

import calendar
import time
import argparse
# Removed cryptography import - no longer needed for credential storage
from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ExtractionPipeline
from cognite.client.data_classes import ExtractionPipelineRun
from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError
from datetime import datetime
import sys
import os

# Add the parent directory to the path so we can import from src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import Wordfeud API from local files (included in zip)
from wordfeud_api import Wordfeud

# Import credentials (for local development only)
try:
    from credentials import EMAIL, PASSWORD, USERNAME
    LOCAL_CREDENTIALS_AVAILABLE = True
except ImportError:
    EMAIL = PASSWORD = USERNAME = None
    LOCAL_CREDENTIALS_AVAILABLE = False

GLOBAL_CLIENT = None

def create_time_series(client, dataset_id, username):
    """Create time series for Wordfeud data"""
    time_series = [
        TimeSeries(name=f'Wordfeud Rating - {username}', external_id=f'WORDFEUD/{username}/rating', unit='rating', is_step=True),
        TimeSeries(name=f'Wordfeud Games Played - {username}', external_id=f'WORDFEUD/{username}/games_played', unit='count', is_step=True),
        TimeSeries(name=f'Wordfeud Games Won - {username}', external_id=f'WORDFEUD/{username}/games_won', unit='count', is_step=True),
        TimeSeries(name=f'Wordfeud Win Rate - {username}', external_id=f'WORDFEUD/{username}/win_rate', unit='percentage', is_step=True),
        TimeSeries(name=f'Wordfeud Current Streak - {username}', external_id=f'WORDFEUD/{username}/current_streak', unit='count', is_step=True),
        TimeSeries(name=f'Wordfeud Best Rating - {username}', external_id=f'WORDFEUD/{username}/best_rating', unit='rating', is_step=True)
    ]
    
    for ts in time_series:
        if dataset_id != -1:
            ts.data_set_id = dataset_id
        try:
            client.time_series.create(ts)
        except CogniteDuplicatedError as err:
            print(f'{ts.external_id} already exists')

def delete_existing_timeseries(client, username):
    """Delete existing time series for the given username"""
    external_ids = [
        f'WORDFEUD/{username}/rating',
        f'WORDFEUD/{username}/games_played',
        f'WORDFEUD/{username}/games_won',
        f'WORDFEUD/{username}/win_rate',
        f'WORDFEUD/{username}/current_streak',
        f'WORDFEUD/{username}/best_rating'
    ]
    
    existing_timeseries = []
    for external_id in external_ids:
        try:
            ts = client.time_series.retrieve(external_id=external_id)
            existing_timeseries.append(ts)
        except Exception:
            # Time series doesn't exist, skip
            pass
    
    if existing_timeseries:
        print(f"Found {len(existing_timeseries)} existing time series for user '{username}':")
        for ts in existing_timeseries:
            print(f"  - {ts.name} ({ts.external_id})")
        
        response = input(f"\n❓ Do you want to delete these time series? (yes/no): ")
        if response.lower() == 'yes':
            try:
                client.time_series.delete(external_id=[ts.external_id for ts in existing_timeseries])
                print("✅ Successfully deleted existing time series")
                return True
            except Exception as e:
                print(f"❌ Error deleting time series: {e}")
                return False
        else:
            print("❌ Deletion cancelled")
            return False
    else:
        print(f"✅ No existing time series found for user '{username}'")
        return True

def create_extraction_pipeline(client, extraction_pipeline, dataset_id, username):
    """Create extraction pipeline for Wordfeud data"""
    extpipe = ExtractionPipeline(
        name=f"Wordfeud Extractor - {username}",
        external_id=extraction_pipeline, 
        description=f'Wordfeud to CDF extractor for {username}'
    )
    if dataset_id != -1:
        extpipe.data_set_id = dataset_id
    try:
        client.extraction_pipelines.create(extpipe)
    except CogniteDuplicatedError as err:
        print('Extraction pipeline %s already exists' % extraction_pipeline)

def report_extraction_pipeline_run(client, extraction_pipeline, status='success', message=None):
    """Report extraction pipeline run status"""
    extpiperun = ExtractionPipelineRun(status=status, extpipe_external_id=extraction_pipeline)
    if message:
        extpiperun.message = message
    client.extraction_pipelines.runs.create(extpiperun)

def get_latest_datapoint(client, external_id):
    """Get the latest datapoint from a time series"""
    try:
        # Get the latest datapoint
        datapoints = client.time_series.data.retrieve_latest(
            external_id=external_id,
            before=0  # Get the most recent datapoint
        )
        if datapoints and len(datapoints) > 0:
            return datapoints[0]
        return None
    except Exception as e:
        print(f"Could not retrieve latest datapoint for {external_id}: {e}")
        return None

def get_wordfeud_data(wordfeud_client, client, username, start_time, end_time):
    """Fetch Wordfeud data and only create datapoints for completed games"""
    datapoints = {
        'rating': [],
        'games_played': [],
        'games_won': [],
        'win_rate': [],
        'current_streak': [],
        'best_rating': []
    }
    
    try:
        # Get current user status and rating
        status = wordfeud_client.get_status()
        current_time = int(time.time() * 1000)
        
        # Get current rating
        try:
            rating_data = wordfeud_client.get_current_rating()
            if not rating_data or 'rating' not in rating_data:
                print("No rating data available")
                return datapoints
        except Exception as e:
            print(f"Could not retrieve rating data: {e}")
            return datapoints
        
        current_rating = rating_data['rating']
        current_best_rating = rating_data.get('best_rating', current_rating)
        
        # Get the latest stored rating to compare
        rating_external_id = f'WORDFEUD/{username}/rating'
        latest_rating_datapoint = get_latest_datapoint(client, rating_external_id)
        
        if latest_rating_datapoint:
            last_rating = latest_rating_datapoint.value
            last_timestamp = latest_rating_datapoint.timestamp
            
            # Get games with rating information (finished games)
            games_with_ratings = wordfeud_client.get_ratings()
            if games_with_ratings:
                # Find games that were completed after the last datapoint
                new_rating_games = []
                for game in games_with_ratings:
                    if game.get('rating') is not None and game.get('end_game'):
                        game_finished_time = int(game['end_game']) * 1000  # Convert to milliseconds
                        if game_finished_time > last_timestamp:
                            new_rating_games.append(game)
                
                if new_rating_games:
                    # Sort games by finish time to process them chronologically
                    new_rating_games.sort(key=lambda g: int(g['end_game']))
                    
                    print(f"Found {len(new_rating_games)} new completed games")
                    
                    # Create datapoint for each completed game (regardless of rating change)
                    for game in new_rating_games:
                        game_finished_time = int(game['end_game']) * 1000
                        game_rating = game.get('rating')
                        rating_delta = game.get('rating_delta', 0)
                        
                        print(f"Game {game.get('id')}: Rating {game_rating} (change: {rating_delta}) at {game['end_game']}")
                        
                        # Store rating datapoint with game metadata
                        datapoints['rating'].append({
                            'timestamp': game_finished_time,
                            'value': game_rating,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game.get('result', 'unknown'),
                                'opponent': game.get('opponent', 'unknown'),
                                'rating_delta': rating_delta,
                                'ruleset': game.get('ruleset'),
                                'board': game.get('board')
                            }
                        })
                        
                        # Update other metrics based on this game
                        # Get current total games count
                        all_games = wordfeud_client.get_games()
                        if all_games:
                            total_games = len(all_games)
                            won_games = sum(1 for g in all_games if g.get('result') == 'won')
                            win_rate = (won_games / total_games * 100) if total_games > 0 else 0
                            
                            datapoints['games_played'].append({
                                'timestamp': game_finished_time,
                                'value': total_games,
                                'metadata': {
                                    'game_id': game.get('id', 'unknown'),
                                    'result': game.get('result', 'unknown'),
                                    'rating_delta': rating_delta
                                }
                            })
                            
                            datapoints['games_won'].append({
                                'timestamp': game_finished_time,
                                'value': won_games,
                                'metadata': {
                                    'game_id': game.get('id', 'unknown'),
                                    'result': game.get('result', 'unknown'),
                                    'rating_delta': rating_delta
                                }
                            })
                            
                            datapoints['win_rate'].append({
                                'timestamp': game_finished_time,
                                'value': win_rate,
                                'metadata': {
                                    'game_id': game.get('id', 'unknown'),
                                    'result': game.get('result', 'unknown'),
                                    'rating_delta': rating_delta
                                }
                            })
                            
                            # Update best rating if this game improved it
                            if game_rating > last_rating:
                                datapoints['best_rating'].append({
                                    'timestamp': game_finished_time,
                                    'value': game_rating,
                                    'metadata': {
                                        'game_id': game.get('id', 'unknown'),
                                        'result': game.get('result', 'unknown'),
                                        'rating_delta': rating_delta
                                    }
                                })
                else:
                    print("No new completed games found")
            else:
                print("No games with rating information available")
        else:
            # First run - create initial datapoint
            print(f"Creating initial datapoint with rating: {current_rating}")
            datapoints['rating'].append({
                'timestamp': current_time,
                'value': current_rating,
                'metadata': {'initial': True}
            })
            
            datapoints['best_rating'].append({
                'timestamp': current_time,
                'value': current_best_rating,
                'metadata': {'initial': True}
            })
            
            # Get initial games data
            games = wordfeud_client.get_games()
            if games:
                total_games = len(games)
                won_games = sum(1 for game in games if game.get('result') == 'won')
                win_rate = (won_games / total_games * 100) if total_games > 0 else 0
                
                datapoints['games_played'].append({
                    'timestamp': current_time,
                    'value': total_games,
                    'metadata': {'initial': True}
                })
                
                datapoints['games_won'].append({
                    'timestamp': current_time,
                    'value': won_games,
                    'metadata': {'initial': True}
                })
                
                datapoints['win_rate'].append({
                    'timestamp': current_time,
                    'value': win_rate,
                    'metadata': {'initial': True}
                })
        
        print(f"Successfully processed Wordfeud data: {sum(len(d) for d in datapoints.values())} new datapoints")
        
    except Exception as e:
        print(f"Error fetching Wordfeud data: {e}")
    
    return datapoints

def store_wordfeud_data(client, data, username):
    """Store Wordfeud data in CDF with metadata"""
    ts_point_list = []
    
    for metric, datapoints in data.items():
        if datapoints:
            external_id = f'WORDFEUD/{username}/{metric}'
            
            # Convert datapoints to CDF format
            cdf_datapoints = []
            for dp in datapoints:
                if isinstance(dp, dict):
                    # New format with metadata
                    cdf_datapoint = {
                        'timestamp': dp['timestamp'],
                        'value': dp['value']
                    }
                    if 'metadata' in dp:
                        cdf_datapoint['metadata'] = dp['metadata']
                    cdf_datapoints.append(cdf_datapoint)
                else:
                    # Legacy format (timestamp, value)
                    cdf_datapoints.append({'timestamp': dp[0], 'value': dp[1]})
            
            if cdf_datapoints:
                ts_point_list.append({
                    'externalId': external_id, 
                    'datapoints': cdf_datapoints
                })
    
    if ts_point_list:
        client.time_series.data.insert_multiple(ts_point_list)
        print(f"Successfully stored {len(ts_point_list)} time series to CDF")

def handle(data, client, secrets):
    """Main handler function for the CDF function"""
    global GLOBAL_CLIENT
    GLOBAL_CLIENT = client
    
    try:
        # Get credentials from function secrets (production) or credentials file (local development)
        email = secrets.get('wordfeud-email')
        password = secrets.get('wordfeud-pass')
        username = secrets.get('wordfeud-user')
        
        # Fall back to local credentials file for local development
        if not email and LOCAL_CREDENTIALS_AVAILABLE:
            email = EMAIL
        if not password and LOCAL_CREDENTIALS_AVAILABLE:
            password = PASSWORD
        if not username and LOCAL_CREDENTIALS_AVAILABLE:
            username = USERNAME
        
        if not email or not password or not username:
            raise Exception("Wordfeud credentials not found. Please configure function secrets (wordfeud-email, wordfeud-pass, wordfeud-user) or ensure credentials.py exists for local development.")
        
        # Initialize Wordfeud client with board configuration
        wordfeud_client = Wordfeud()
        wordfeud_client.login_email(email, password)
        
        # Configure board type and rule set
        board_type = data.get('board_type', secrets.get('board-type', 'BoardNormal'))
        rule_set = data.get('rule_set', secrets.get('rule-set', 'RuleSetNorwegian'))
        
        # Set board configuration
        wordfeud_client.board_type = getattr(wordfeud_client, board_type)
        wordfeud_client.rule_set = getattr(wordfeud_client, rule_set)
        
        print(f"✓ Wordfeud login successful")
        print(f"✓ Board configured: {board_type}, {rule_set}")
        
        # Determine time range
        start_time = (int(time.time()*1000) - 7*24*3600000) - ((int(time.time()*1000) - 7*24*3600000) % (3600000))
        if 'start-time' in data:
            start_time = int(data['start-time'])

        end_time = int(time.time()*1000)
        if 'end-time' in data:
            end_time = int(data['end-time'])

        start_date = datetime.utcfromtimestamp(start_time/1000).strftime("%Y-%m-%d")
        end_date = datetime.utcfromtimestamp(end_time/1000).strftime("%Y-%m-%d")
        print(f'Starting Wordfeud data extraction from {start_date} to {end_date}')

        # Get and store data
        wordfeud_data = get_wordfeud_data(wordfeud_client, client, username, start_time, end_time)
        
        if any(wordfeud_data.values()):
            store_wordfeud_data(client, wordfeud_data, username)
            print(f'Successfully processed Wordfeud data for {username}')
        else:
            print('No Wordfeud data was collected for the specified time range')

        # Report extraction pipeline run
        if 'extraction-pipeline' in data:
            report_extraction_pipeline_run(client, data['extraction-pipeline'])
            
    except Exception as e:
        error_msg = f'Critical error in handle function: {type(e).__name__}: {str(e)}'
        print(error_msg)
        
        # Report failure to extraction pipeline if configured
        if 'extraction-pipeline' in data:
            try:
                report_extraction_pipeline_run(client, data['extraction-pipeline'], status='failure', message=error_msg)
            except Exception as pipeline_error:
                print(f'Failed to report pipeline failure: {pipeline_error}')
        
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument( 
        '-k', '--key', type=str, required=True, help='Cognite OIDC client secret. Required.')
    parser.add_argument( 
        '-c', '--client_id', type=str, required=True, help='Cognite OIDC client id. Required.')
    parser.add_argument( 
        '-t', '--tenant_id', type=str, help='Tenant id in identity provider used for OIDC. Required for Azure AD, optional for other IDPs.')
    parser.add_argument(
        '-p', '--project', type=str, required=True, help='Project name. Required.')
    parser.add_argument(
        '-b', '--base_url', type=str, help='Base URL of the CDF cluster of the CDF project. Defaults to https://api.cognitedata.com.', default='https://api.cognitedata.com')
    parser.add_argument(
        '--token_url', type=str, help='Token URL of the CDF project. If not set, it will use the default Azure AD token url based on the tenant ID provided.')
    parser.add_argument(
        '-s', '--start_time', type=int, help='Begin at this UTC unix timestamp in ms. Defaults to one week ago.', default=-1)
    parser.add_argument(
        '-e', '--end_time', type=int, help='End at this UTC unix timestamp in ms. Defaults to now.', default=-1)    
    parser.add_argument(
        '-i', '--init', type=bool, help='Create necessary time series and extraction pipeline, but do not do anything else.', default=False)
    parser.add_argument(
        '--cleanup', action='store_true', help='Delete existing time series before creating new ones (requires --init)')
    parser.add_argument(
        '-d', '--dataset', type=int, help='Dataset ID from Cognite Data Fusion', default=-1)
    parser.add_argument(
        '-a', '--admin_security_category', type=int, help='ID of admin security category for the Wordfeud credentials', default=-1)
    parser.add_argument(
        '--extraction_pipeline', type=str, help='External ID of extraction pipeline to update on every run', default='extractors/wordfeud-USERNAME')
    # Credentials are now loaded from credentials.py file
    parser.add_argument(
        '--board_type', type=str, choices=['BoardNormal', 'BoardRandom'], default='BoardNormal',
        help='Wordfeud board type. Default: BoardNormal')
    parser.add_argument(
        '--rule_set', type=str, 
        choices=['RuleSetAmerican', 'RuleSetDanish', 'RuleSetDutch', 'RuleSetEnglish', 
                'RuleSetFrench', 'RuleSetNorwegian', 'RuleSetSpanish', 'RuleSetSwedish'],
        default='RuleSetNorwegian', help='Wordfeud rule set/language. Default: RuleSetNorwegian')

    args = parser.parse_args()
    if args.start_time == -1:
        args.start_time = (int(time.time()*1000) - 7*24*3600000) - ((int(time.time()*1000) - 7*24*3600000) % (3600000))
    if args.end_time == -1:
        args.end_time = int(time.time()*1000)

    SCOPES = ["%s/.default" % args.base_url]
    if args.token_url:
        TOKEN_URL = args.token_url
    elif args.tenant_id:
        # Azure AD configuration
        TOKEN_URL = "https://login.microsoftonline.com/%s/oauth2/v2.0/token" % args.tenant_id
    else:
        # Other IDP configuration - token URL must be provided explicitly
        raise ValueError("Either --token_url or --tenant_id must be provided for IDP configuration")

    creds = OAuthClientCredentials(
        token_url=TOKEN_URL,
        client_id=args.client_id,
        client_secret=args.key,
        scopes=SCOPES,
        audience=args.base_url)
    config = ClientConfig(
        client_name='wordfeud-rating-reader',
        project=args.project,
        base_url=args.base_url,
        credentials=creds)
    client = CogniteClient(config)

    if args.init:
        # For initialization, use credentials from file or command line
        init_username = USERNAME if LOCAL_CREDENTIALS_AVAILABLE else None
        
        if not init_username:
            print("❌ Error: USERNAME not found in credentials.py")
            print("Please add USERNAME to your credentials.py file for local initialization")
            sys.exit(1)
        
        # Handle cleanup if requested
        if args.cleanup:
            print(f"🧹 Cleanup mode: Checking for existing time series for user '{init_username}'...")
            if not delete_existing_timeseries(client, init_username):
                print("❌ Cleanup failed or was cancelled. Exiting.")
                sys.exit(1)
            print("✅ Cleanup completed successfully")
        
        create_time_series(client, args.dataset, init_username)
        create_extraction_pipeline(client, args.extraction_pipeline, args.dataset, init_username)
        print(f"✓ Time series and extraction pipeline created successfully for user: {init_username}")
        print("ℹ Wordfeud credentials are loaded from credentials.py file for initialization")
    else:
        # For local testing and CDF function execution, credentials are loaded from credentials.py
        secrets = {}
        data = {}
        data['start-time'] = args.start_time
        data['end-time'] = args.end_time
        data['extraction-pipeline'] = args.extraction_pipeline
        data['board_type'] = args.board_type
        data['rule_set'] = args.rule_set
        handle(data, client, secrets) 