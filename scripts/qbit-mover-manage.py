#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
import json
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Use -v for DEBUG
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

try:
    from qbittorrentapi import APIConnectionError, Client, LoginFailed, TorrentDictionary
except ModuleNotFoundError:
    logging.error(
        'Requirements Error: qbittorrent-api not installed. Please install using the command "pip install qbittorrent-api"'
    )
    sys.exit(1)

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qbit-mover-manage.py",
        description="Manages torrents before/after Unraid mover process (best used with Mover Tuning plugin)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "action",
        choices=['pause', 'resume'],
        help="action to perform: 'pause' before mover runs, 'resume' after mover completes"
    )
    
    # Connection parameters required for both actions
    connection_params = parser.add_argument_group('connection parameters (required)')
    connection_params.add_argument(
        "--host",
        help="qBittorrent host, including :port if needed",
        required=True
    )
    connection_params.add_argument(
        "-u", "--user",
        metavar="USER",
        help="qBittorrent user",
        default="admin"
    )
    connection_params.add_argument(
        "-p", "--password",
        metavar="PASS",
        help="qBittorrent password",
        default="adminadmin"
    )
    
    # Create parameter group for pause-only parameters
    pause_params = parser.add_argument_group('filter parameters (only required for pause action)')
    pause_params.add_argument(
        "--cache-mount",
        metavar="PATH",
        help="cache mount point in Unraid. Filters for torrents that exist on the cache mount.",
        default=None,
    )
    pause_params.add_argument(
        "--days-from",
        metavar="DAYS",
        type=int,
        default=0,
        help="include torrents newer than this many days"
    )
    pause_params.add_argument(
        "--days-to",
        metavar="DAYS",
        type=int,
        default=2,
        help="include torrents older than this many days"
    )
    pause_params.add_argument(
        "--status-filter",
        help="define a status to limit which torrents to pause",
        choices=[
            "all", "downloading", "seeding", "completed", "paused",
            "stopped", "active", "inactive", "resumed", "running",
            "stalled", "stalled_uploading", "stalled_downloading",
            "checking", "moving", "errored",
        ],
        default="completed",
    )
    
    # Add state file parameter that works with both actions
    parser.add_argument(
        "--state-file",
        metavar="PATH",
        help="file to store/read torrent state",
        default="./qbit_mover_state.json"
    )

    # Add verbose logging option
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="enable verbose logging"
    )
    
    return parser

def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Validate command line arguments based on action"""
    if args.action == 'resume':
        # Check if any pause-only parameters were provided
        pause_params = ['cache_mount', 'days_from', 'days_to', 'status_filter']
        provided_params = [p for p in pause_params 
                         if getattr(args, p) != parser.get_default(p)]
        if provided_params:
            raise ValueError(
                f"The following parameters are not valid with 'resume' action: "
                f"{', '.join(provided_params)}"
            )

def filter_torrents(
    torrent_list: List[TorrentDictionary],
    timeoffset_from: float,
    timeoffset_to: float,
    cache_mount: str | None
) -> List[TorrentDictionary]:
    """Filter torrents based on age and cache location"""
    result = []
    for torrent in torrent_list:
        if timeoffset_to <= torrent.added_on <= timeoffset_from:
            if not cache_mount or exists_in_cache(cache_mount, torrent.content_path):
                result.append(torrent)
        elif torrent.added_on < timeoffset_to:
            break  # Since list is sorted by added_on in reverse order
    return result

def exists_in_cache(cache_mount: str, content_path: str) -> bool:
    """Check if torrent content exists in cache location"""
    cache_path = os.path.join(cache_mount, content_path.lstrip("/"))
    return os.path.exists(cache_path)

def save_torrent_state(
    torrents: List[TorrentDictionary],
    state_file: str
) -> None:
    """Save torrent IDs for later resumption"""
    state = {
        'torrent_hashes': [t.hash for t in torrents],
        'timestamp': datetime.now().isoformat(),
        'version': '1.0' # For future compatibility checking
    }
    try:
        # First check if directory exists/is writable
        state_dir = os.path.dirname(os.path.abspath(state_file))
        if not os.path.exists(state_dir):
            raise OSError(f"Directory does not exist: {state_dir}")
        if not os.access(state_dir, os.W_OK):
            raise OSError(f"Directory is not writable: {state_dir}")
            
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    except (OSError, IOError) as e:
        raise RuntimeError(f"Failed to save state file: {str(e)}")

def load_torrent_state(state_file: str) -> Dict[str, Any]:
    """Load saved torrent state"""
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
            
        # Basic state file validation
        required_keys = {'torrent_hashes', 'timestamp'}
        if not all(key in state for key in required_keys):
            raise ValueError("State file is missing required fields")
            
        return state
            
    except FileNotFoundError:
        raise RuntimeError(
            f"No state file found at {state_file}. Did you run with 'pause' first?"
        )
    except json.JSONDecodeError:
        raise RuntimeError(f"State file {state_file} is corrupted")
    except IOError as e:
        raise RuntimeError(f"Failed to read state file: {str(e)}")

def manage_torrents(client: Client, torrent_list: List[TorrentDictionary], pause: bool) -> None:
    """Pause or resume torrents"""
    action = "Pausing" if pause else "Resuming"
    for torrent in torrent_list:
        logging.info(f"{action}: {torrent.name} [{datetime.fromtimestamp(torrent.added_on)}]")
        if pause:
            torrent.pause()
        else:
            torrent.resume()

def main() -> int:
    parser = create_parser()
    args = parser.parse_args()
    
    # Set debug logging if verbose flag is provided
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        validate_args(args, parser)
        
        if args.action == 'pause':
            if args.days_from > args.days_to:
                raise ValueError("Config Error: days_from must be set lower than days_to")

            try:
                client = Client(host=args.host, username=args.user, password=args.password)
            except LoginFailed:
                raise RuntimeError("qBittorrent Error: Failed to login. Invalid username/password.")
            except APIConnectionError:
                raise RuntimeError("qBittorrent Error: Unable to connect to the client.")
            except Exception as e:
                raise RuntimeError(f"qBittorrent Error: Unable to connect to the client. {str(e)}")

            current = datetime.now()
            timeoffset_from = current - timedelta(days=args.days_from)
            timeoffset_to = current - timedelta(days=args.days_to)
            torrent_list = client.torrents.info(
                status_filter=args.status_filter,
                sort="added_on",
                reverse=True
            )
            
            torrents = filter_torrents(
                torrent_list,
                timeoffset_from.timestamp(),
                timeoffset_to.timestamp(),
                args.cache_mount
            )
            
            if not torrents:
                logging.warning("No matching torrents found to pause")
            else:
                logging.info(f"Found {len(torrents)} matching torrents from {args.days_from} - {args.days_to} days ago")
                manage_torrents(client, torrents, pause=True)
                save_torrent_state(torrents, args.state_file)
                logging.info(f"Torrent state saved to {args.state_file}")

        if args.action == 'resume':
            saved_state = load_torrent_state(args.state_file)
            
            try:
                client = Client(
                    host=args.host,
                    username=args.user,
                    password=args.password
                )
            except Exception as e:
                raise RuntimeError(f"Failed to connect to qBittorrent: {str(e)}")

            torrents = [
                t for t in client.torrents.info()
                if t.hash in saved_state['torrent_hashes']
            ]
            
            if not torrents:
                logging.warning("No saved torrents found to resume")
            else:
                logging.info(f"Resuming {len(torrents)} paused torrents")
                manage_torrents(client, torrents, pause=False)
            
            # Clean up state file
            try:
                os.remove(args.state_file)
                logging.debug(f"Cleaned up state file: {args.state_file}")
            except OSError as e:
                logging.warning(f"Failed to clean up state file: {str(e)}")

        return 0

    except Exception as e:
        logging.error(str(e))
        return 1

if __name__ == "__main__":
    sys.exit(main()) 