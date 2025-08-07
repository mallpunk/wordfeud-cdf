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
        # Get games with rating information (finished games)
        games_with_ratings = wordfeud_client.get_ratings()
        if not games_with_ratings:
            print("No games with rating information available")
            return datapoints
        
        # Get the latest stored rating to determine the last processed timestamp
        rating_external_id = f'WORDFEUD/{username}/rating'
        latest_rating_datapoint = get_latest_datapoint(client, rating_external_id)
        
        if latest_rating_datapoint:
            # We have existing data - find new games since the last datapoint
            last_timestamp = latest_rating_datapoint.timestamp
            last_datapoint_date = datetime.utcfromtimestamp(last_timestamp/1000).strftime("%Y-%m-%d %H:%M:%S")
            print(f"Found latest datapoint in time series {rating_external_id}: timestamp={last_timestamp} ({last_datapoint_date}), value={latest_rating_datapoint.value}")
            
            # Find games that were completed after the last datapoint
            new_rating_games = []
            for game in games_with_ratings:
                if game.get('rating') is not None and game.get('updated'):
                    game_updated = game.get('updated')
                    if game_updated and int(game_updated) > 0:
                        game_finished_time = int(game_updated) * 1000  # Convert to milliseconds
                        if game_finished_time > last_timestamp:
                            new_rating_games.append(game)
            
            if new_rating_games:
                # Sort games by finish time to process them chronologically
                new_rating_games.sort(key=lambda g: int(g['updated']))
                
                print(f"Found {len(new_rating_games)} new completed games")
                print(f"Processing games for time series: WORDFEUD/{username}/rating")
                
                # Track the best rating as we process games
                current_best_rating = latest_rating_datapoint.value
                
                # Create datapoint for each completed game (regardless of rating change)
                for game in new_rating_games:
                    # Use the 'updated' timestamp as the game finish time
                    game_updated = game.get('updated')
                    if not game_updated or int(game_updated) <= 0:
                        print(f"WARNING: Game {game.get('id')} has invalid updated timestamp: {game_updated}, skipping this game")
                        continue
                    
                    # Convert seconds to milliseconds
                    game_finished_time = int(game_updated) * 1000
                    game_rating = game.get('rating')
                    rating_delta = game.get('rating_delta', 0)
                    
                    game_end_date = datetime.utcfromtimestamp(game_finished_time/1000).strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Game {game.get('id')}: Rating {game_rating} (change: {rating_delta}) finished at {game_end_date}")
                    
                    # Extract available metadata from the API response
                    game_metadata = {
                        'game_id': game.get('id', 'unknown'),
                        'rating_delta': rating_delta,
                        'ruleset': game.get('ruleset'),
                        'board': game.get('board'),
                        'move_count': game.get('move_count'),
                        'created': game.get('created'),
                        'updated': game.get('updated')
                    }
                    
                    # Extract opponent information from players array
                    players = game.get('players', [])
                    if players:
                        # Find the opponent (non-local player)
                        for player in players:
                            if not player.get('is_local', False):
                                game_metadata['opponent'] = player.get('username', 'unknown')
                                game_metadata['opponent_score'] = player.get('score', 0)
                                break
                    
                    # Determine game result based on scores
                    local_player = None
                    opponent_player = None
                    for player in players:
                        if player.get('is_local', False):
                            local_player = player
                        else:
                            opponent_player = player
                    
                    if local_player and opponent_player:
                        local_score = local_player.get('score', 0)
                        opponent_score = opponent_player.get('score', 0)
                        if local_score > opponent_score:
                            game_metadata['result'] = 'won'
                        elif local_score < opponent_score:
                            game_metadata['result'] = 'lost'
                        else:
                            game_metadata['result'] = 'tied'
                    
                    # Store rating datapoint with available metadata
                    datapoints['rating'].append({
                        'timestamp': game_finished_time,
                        'value': game_rating,
                        'metadata': game_metadata
                    })
                    
                    # Update best rating if this game improved it
                    if game_rating > current_best_rating:
                        current_best_rating = game_rating
                        datapoints['best_rating'].append({
                            'timestamp': game_finished_time,
                            'value': game_rating,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
                    
                    # Update other metrics based on the state after this game
                    # Get all games to calculate current totals
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
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
                        
                        datapoints['games_won'].append({
                            'timestamp': game_finished_time,
                            'value': won_games,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
                        
                        datapoints['win_rate'].append({
                            'timestamp': game_finished_time,
                            'value': win_rate,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
            else:
                print("No new completed games found")
        else:
            print(f"No existing datapoints found in time series {rating_external_id}")
            
            # First run - check if there are any completed games to create initial datapoints
            completed_games = []
            for game in games_with_ratings:
                if game.get('rating') is not None and game.get('updated'):
                    completed_games.append(game)
            
            if completed_games:
                # Sort games by finish time to process them chronologically
                completed_games.sort(key=lambda g: int(g['updated']))
                
                print(f"First run: Found {len(completed_games)} completed games to create initial datapoints")
                
                # Track the best rating as we process games
                current_best_rating = 0
                
                # Create datapoint for each completed game
                for game in completed_games:
                    # Use the 'updated' timestamp as the game finish time
                    game_updated = game.get('updated')
                    if not game_updated or int(game_updated) <= 0:
                        print(f"WARNING: Game {game.get('id')} has invalid updated timestamp: {game_updated}, skipping this game")
                        continue
                    
                    # Convert seconds to milliseconds
                    game_finished_time = int(game_updated) * 1000
                    game_rating = game.get('rating')
                    rating_delta = game.get('rating_delta', 0)
                    
                    game_end_date = datetime.utcfromtimestamp(game_finished_time/1000).strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Game {game.get('id')}: Rating {game_rating} (change: {rating_delta}) finished at {game_end_date}")
                    
                    # Extract available metadata from the API response
                    game_metadata = {
                        'game_id': game.get('id', 'unknown'),
                        'rating_delta': rating_delta,
                        'ruleset': game.get('ruleset'),
                        'board': game.get('board'),
                        'move_count': game.get('move_count'),
                        'created': game.get('created'),
                        'updated': game.get('updated')
                    }
                    
                    # Extract opponent information from players array
                    players = game.get('players', [])
                    if players:
                        # Find the opponent (non-local player)
                        for player in players:
                            if not player.get('is_local', False):
                                game_metadata['opponent'] = player.get('username', 'unknown')
                                game_metadata['opponent_score'] = player.get('score', 0)
                                break
                    
                    # Determine game result based on scores
                    local_player = None
                    opponent_player = None
                    for player in players:
                        if player.get('is_local', False):
                            local_player = player
                        else:
                            opponent_player = player
                    
                    if local_player and opponent_player:
                        local_score = local_player.get('score', 0)
                        opponent_score = opponent_player.get('score', 0)
                        if local_score > opponent_score:
                            game_metadata['result'] = 'won'
                        elif local_score < opponent_score:
                            game_metadata['result'] = 'lost'
                        else:
                            game_metadata['result'] = 'tied'
                    
                    # Store rating datapoint with available metadata
                    datapoints['rating'].append({
                        'timestamp': game_finished_time,
                        'value': game_rating,
                        'metadata': game_metadata
                    })
                    
                    # Update best rating if this game improved it
                    if game_rating > current_best_rating:
                        current_best_rating = game_rating
                        datapoints['best_rating'].append({
                            'timestamp': game_finished_time,
                            'value': game_rating,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
                    
                    # Update other metrics based on the state after this game
                    # Get all games to calculate current totals
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
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
                        
                        datapoints['games_won'].append({
                            'timestamp': game_finished_time,
                            'value': won_games,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
                        
                        datapoints['win_rate'].append({
                            'timestamp': game_finished_time,
                            'value': win_rate,
                            'metadata': {
                                'game_id': game.get('id', 'unknown'),
                                'result': game_metadata.get('result', 'unknown'),
                                'rating_delta': rating_delta
                            }
                        })
            else:
                print("First run: No completed games found - no initial datapoints created")
        
        total_datapoints = sum(len(d) for d in datapoints.values())
        print(f"Successfully processed Wordfeud data: {total_datapoints} new datapoints")
        if total_datapoints > 0:
            for metric, points in datapoints.items():
                if points:
                    external_id = f'WORDFEUD/{username}/{metric}'
                    print(f"  - {external_id}: {len(points)} datapoints")
        
    except Exception as e:
        print(f"Error processing Wordfeud data: {e}")
        import traceback
        traceback.print_exc()
    
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
                # Check if time series exists before inserting
                try:
                    ts_info = client.time_series.retrieve(external_id=external_id)
                    print(f"✓ Time series {external_id} exists")
                except Exception as ts_error:
                    print(f"❌ Time series {external_id} does not exist: {ts_error}")
                    print(f"  Skipping data insertion for {external_id}")
                    continue
                
                ts_point_list.append({
                    'externalId': external_id, 
                    'datapoints': cdf_datapoints
                })
    
    if ts_point_list:
        try:
            print(f"Attempting to insert {len(ts_point_list)} time series to CDF...")
            for ts_data in ts_point_list:
                external_id = ts_data['externalId']
                datapoint_count = len(ts_data['datapoints'])
                print(f"  - {external_id}: {datapoint_count} datapoints")
                # Log first datapoint for debugging
                if ts_data['datapoints']:
                    first_dp = ts_data['datapoints'][0]
                    print(f"    First datapoint: timestamp={first_dp['timestamp']}, value={first_dp['value']}")
            
            # Insert the data
            print(f"Data structure being sent to CDF:")
            for ts_data in ts_point_list:
                external_id = ts_data['externalId']
                datapoints = ts_data['datapoints']
                print(f"  {external_id}: {len(datapoints)} datapoints")
                if datapoints:
                    first_dp = datapoints[0]
                    print(f"    Sample datapoint: {first_dp}")
            
            result = client.time_series.data.insert_multiple(ts_point_list)
            print(f"✓ CDF insert_multiple completed successfully")
            
            # Verify the insertion by checking if time series has data
            for ts_data in ts_point_list:
                external_id = ts_data['externalId']
                try:
                    # Get the latest datapoint to verify insertion
                    latest = client.time_series.data.retrieve_latest(external_id=external_id)
                    if latest and len(latest) > 0:
                        print(f"✓ Verified: {external_id} has data (latest: {latest[0].timestamp})")
                    else:
                        print(f"⚠️  Warning: {external_id} appears to be empty after insertion")
                except Exception as verify_error:
                    print(f"⚠️  Warning: Could not verify {external_id}: {verify_error}")
                    
        except Exception as insert_error:
            print(f"❌ Error inserting data to CDF: {insert_error}")
            print(f"Error type: {type(insert_error).__name__}")
            # Log the data that failed to insert
            for ts_data in ts_point_list:
                external_id = ts_data['externalId']
                datapoint_count = len(ts_data['datapoints'])
                print(f"  Failed to insert: {external_id} ({datapoint_count} datapoints)")
            raise

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
        extraction_pipeline = data.get('extraction-pipeline')
        if not extraction_pipeline and username:
            # Auto-generate extraction pipeline name from username
            extraction_pipeline = f'extractors/wordfeud-{username}'
        
        if extraction_pipeline:
            report_extraction_pipeline_run(client, extraction_pipeline)
            
    except Exception as e:
        error_msg = f'Critical error in handle function: {type(e).__name__}: {str(e)}'
        print(error_msg)
        
        # Report failure to extraction pipeline if configured
        extraction_pipeline = data.get('extraction-pipeline')
        if not extraction_pipeline and username:
            # Auto-generate extraction pipeline name from username
            extraction_pipeline = f'extractors/wordfeud-{username}'
        
        if extraction_pipeline:
            try:
                report_extraction_pipeline_run(client, extraction_pipeline, status='failure', message=error_msg)
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
        '--extraction_pipeline', type=str, help='External ID of extraction pipeline to update on every run', default=None)
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
        
        # Set default extraction pipeline external ID if not provided
        if args.extraction_pipeline is None:
            args.extraction_pipeline = f'extractors/wordfeud-{init_username}'
        
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
        
        # Set default extraction pipeline external ID if not provided
        if args.extraction_pipeline is None:
            init_username = USERNAME if LOCAL_CREDENTIALS_AVAILABLE else None
            if init_username:
                args.extraction_pipeline = f'extractors/wordfeud-{init_username}'
        
        data['extraction-pipeline'] = args.extraction_pipeline
        data['board_type'] = args.board_type
        data['rule_set'] = args.rule_set
        handle(data, client, secrets) 