"""Market discovery module for finding esports markets on Polymarket."""

import json
import requests
from typing import List, Dict, Any, Optional, Set

from .config import GAMMA_API_URL, GAME_CONFIGS, VALID_GAMES, get_game_config
from .database import upsert_market, get_all_markets, upsert_game_id_mapping
from .utils import rate_limiter, with_retry, logger


def is_game_related(event: Dict[str, Any], market: Dict[str, Any], game: str) -> bool:
    """Check if an event/market is related to a specific game.

    Uses two-tier validation for games with shared team names (e.g. CS2):
    - validation_terms: game/league identifiers that are sufficient alone
    - team_terms: team names that only match if a game_term is also present
    """
    config = get_game_config(game)
    validation_terms = config["validation_terms"]
    team_terms = config.get("team_terms", [])
    game_terms = config.get("game_terms", [])

    # Combine text from event and market for checking
    text_to_check = " ".join([
        event.get("title", ""),
        event.get("description", ""),
        market.get("question", ""),
        market.get("description", ""),
        market.get("groupItemTitle", ""),
    ]).lower()

    # Check validation_terms first — any match is sufficient
    for term in validation_terms:
        if term.lower() in text_to_check:
            return True

    # If there are team_terms, they only match if a game_term is also present
    if team_terms and game_terms:
        has_game_term = any(gt.lower() in text_to_check for gt in game_terms)
        if has_game_term:
            for term in team_terms:
                if term.lower() in text_to_check:
                    return True

    return False


@with_retry
def public_search(query: str, page: int = 1, limit: int = 100) -> Dict[str, Any]:
    """
    Search for events and markets using public-search endpoint with pagination.

    Args:
        query: Search query string
        page: Page number (1-indexed)
        limit: Results per page (max 100)
    """
    rate_limiter.wait_if_needed("gamma_search")

    url = f"{GAMMA_API_URL}/public-search"
    params = {
        "q": query,
        "_limit": min(limit, 100),
        "limit_per_type": min(limit, 100),
    }

    # Add page parameter if not first page
    if page > 1:
        params["page"] = page

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


@with_retry
def fetch_all_tags() -> List[Dict[str, Any]]:
    """Fetch all available tags from the API."""
    rate_limiter.wait_if_needed("gamma_tags")

    url = f"{GAMMA_API_URL}/tags"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Handle different response formats
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return data.get("tags", []) or data.get("data", [])

    return []


def find_esports_tags(tags: List[Dict[str, Any]], tag_labels: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Find tags that may contain esports/gaming markets.

    Args:
        tags: List of tag objects from the API
        tag_labels: Game-specific tag labels to search for. If None, uses all games' labels.

    Returns:
        List of matching tag objects
    """
    if tag_labels is None:
        # Combine all game tag labels
        tag_labels = []
        for config in GAME_CONFIGS.values():
            tag_labels.extend(config["tag_labels"])
        tag_labels = list(set(tag_labels))

    matching_tags = []

    for tag in tags:
        tag_label = (tag.get("label") or tag.get("name") or "").lower()
        tag_slug = (tag.get("slug") or "").lower()

        for search_term in tag_labels:
            if search_term.lower() in tag_label or search_term.lower() in tag_slug:
                matching_tags.append(tag)
                break

    return matching_tags


@with_retry
def fetch_events_by_tag(tag_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch events filtered by tag ID.

    Args:
        tag_id: The tag ID to filter by
        limit: Maximum number of events to fetch
    """
    rate_limiter.wait_if_needed("gamma_events")

    url = f"{GAMMA_API_URL}/events"
    params = {
        "tag_id": tag_id,
        "_limit": limit,
        "closed": False,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return data.get("events", []) or data.get("data", [])

    return []


@with_retry
def get_event_details(event_id: str) -> Optional[Dict[str, Any]]:
    """Fetch detailed information for a specific event."""
    rate_limiter.wait_if_needed("gamma_events")

    url = f"{GAMMA_API_URL}/events/{event_id}"
    response = requests.get(url, timeout=30)

    if response.status_code == 404:
        logger.warning(f"Event {event_id} not found")
        return None

    response.raise_for_status()
    return response.json()


@with_retry
def get_market_details(market_id: str) -> Optional[Dict[str, Any]]:
    """Fetch detailed information for a specific market."""
    rate_limiter.wait_if_needed("gamma_markets")

    url = f"{GAMMA_API_URL}/markets/{market_id}"
    response = requests.get(url, timeout=30)

    if response.status_code == 404:
        logger.warning(f"Market {market_id} not found")
        return None

    response.raise_for_status()
    return response.json()


def parse_json_field(value: Any) -> Any:
    """Parse a field that might be a JSON string."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def extract_market_data(market: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Extract relevant data from a market response."""
    # Handle CLOB token IDs - can be JSON string or list
    clob_token_ids = parse_json_field(market.get("clobTokenIds", []))
    if isinstance(clob_token_ids, list):
        clob_token_id_yes = clob_token_ids[0] if len(clob_token_ids) > 0 else None
        clob_token_id_no = clob_token_ids[1] if len(clob_token_ids) > 1 else None
    else:
        clob_token_id_yes = None
        clob_token_id_no = None

    # Handle outcomes - can be JSON string or list
    outcomes = parse_json_field(market.get("outcomes", []))
    if isinstance(outcomes, str):
        outcomes = [o.strip() for o in outcomes.split(",")]

    # Extract game_id and event_id from market/event objects
    game_id = market.get("gameId") or market.get("game_id")
    event_id = market.get("eventId") or market.get("event_id")
    if event and not event_id:
        event_id = event.get("id")
    if event and not game_id:
        game_id = event.get("gameId") or event.get("game_id")

    return {
        "market_id": market.get("id"),
        "condition_id": market.get("conditionId"),
        "clob_token_id_yes": clob_token_id_yes,
        "clob_token_id_no": clob_token_id_no,
        "question": market.get("question"),
        "outcomes": outcomes,
        "start_date": market.get("startDate"),
        "end_date": market.get("endDate"),
        "event_id": event_id,
        "game_id": game_id,
    }


def discover_game_markets(game: str, include_closed: bool = False) -> List[Dict[str, Any]]:
    """Discover markets for a specific game using search with pagination."""
    config = get_game_config(game)
    display_name = config["display_name"]
    search_terms = config["search_terms"]

    logger.info(f"Starting {display_name} market discovery...")

    all_markets = []
    seen_ids: Set[str] = set()

    for search_term in search_terms:
        logger.info(f"Searching for '{search_term}'...")

        page = 1
        max_pages = 10  # Safety limit

        while page <= max_pages:
            try:
                result = public_search(search_term, page=page)
                events = result.get("events", [])

                if page == 1:
                    logger.info(f"  Found {len(events)} events on page {page}")

                if not events:
                    break

                found_new = False
                for event in events:
                    if event.get("closed", False) and not include_closed:
                        continue

                    markets = event.get("markets", [])

                    for market in markets:
                        market_id = market.get("id")
                        if market_id and market_id not in seen_ids:
                            if market.get("closed", False) and not include_closed:
                                continue

                            if not is_game_related(event, market, game):
                                continue

                            seen_ids.add(market_id)
                            market_data = extract_market_data(market, event)
                            market_data["game"] = game
                            all_markets.append(market_data)
                            found_new = True

                            question = market_data.get("question", "Unknown")[:70]
                            logger.info(f"  Found market: {question}...")

                if not found_new and page > 1:
                    break

                page += 1

            except Exception as e:
                logger.error(f"  Error searching for '{search_term}' page {page}: {e}")
                break

    logger.info(f"Total {display_name} markets found from search: {len(all_markets)}")
    return all_markets


def discover_markets_by_tags(game: Optional[str] = None, include_closed: bool = False) -> List[Dict[str, Any]]:
    """
    Discover markets using tag-based filtering.

    Args:
        game: Specific game to filter for, or None for all games.
        include_closed: Include closed/resolved markets.
    """
    logger.info("Starting tag-based market discovery...")

    all_markets = []
    seen_ids: Set[str] = set()

    # Determine tag labels and games to validate against
    if game:
        tag_labels = get_game_config(game)["tag_labels"]
        games_to_check = [game]
    else:
        tag_labels = None  # find_esports_tags will combine all
        games_to_check = VALID_GAMES

    try:
        tags = fetch_all_tags()
        logger.info(f"Found {len(tags)} total tags")

        esports_tags = find_esports_tags(tags, tag_labels)
        logger.info(f"Found {len(esports_tags)} esports-related tags")

        for tag in esports_tags:
            tag_id = tag.get("id") or tag.get("slug")
            tag_label = tag.get("label") or tag.get("name") or tag_id

            if not tag_id:
                continue

            logger.info(f"Fetching events for tag: {tag_label}")

            try:
                events = fetch_events_by_tag(str(tag_id))
                logger.info(f"  Found {len(events)} events")

                for event in events:
                    if event.get("closed", False) and not include_closed:
                        continue

                    markets = event.get("markets", [])

                    for market in markets:
                        market_id = market.get("id")
                        if market_id and market_id not in seen_ids:
                            if market.get("closed", False) and not include_closed:
                                continue

                            # Try each game to see if it matches
                            matched_game = None
                            for g in games_to_check:
                                if is_game_related(event, market, g):
                                    matched_game = g
                                    break

                            if not matched_game:
                                continue

                            seen_ids.add(market_id)
                            market_data = extract_market_data(market, event)
                            market_data["game"] = matched_game
                            all_markets.append(market_data)

                            question = market_data.get("question", "Unknown")[:70]
                            logger.info(f"  Found [{matched_game.upper()}] market: {question}...")

            except Exception as e:
                logger.error(f"  Error fetching events for tag {tag_label}: {e}")

    except Exception as e:
        logger.error(f"Error in tag-based discovery: {e}")

    logger.info(f"Total markets found from tags: {len(all_markets)}")
    return all_markets


def discover_all_markets(games: Optional[List[str]] = None, include_closed: bool = False) -> List[Dict[str, Any]]:
    """
    Comprehensive market discovery combining search and tag-based methods.

    Args:
        games: List of game keys to discover for. None means all games.
        include_closed: Include closed/resolved markets.

    Returns:
        Deduplicated list of all discovered markets.
    """
    if games is None:
        games = VALID_GAMES

    logger.info(f"Starting comprehensive market discovery for: {', '.join(g.upper() for g in games)}...")

    seen_ids: Set[str] = set()
    all_markets = []

    # Search-based discovery per game
    for game in games:
        search_markets = discover_game_markets(game, include_closed=include_closed)
        for market in search_markets:
            market_id = market.get("market_id")
            if market_id and market_id not in seen_ids:
                seen_ids.add(market_id)
                all_markets.append(market)

    # Tag-based discovery (catches markets search might miss)
    # Pass None for game to check all requested games
    tag_markets = discover_markets_by_tags(game=None, include_closed=include_closed)
    for market in tag_markets:
        market_id = market.get("market_id")
        if market_id and market_id not in seen_ids:
            # Only include if the market's game is in our requested list
            if market.get("game") in games:
                seen_ids.add(market_id)
                all_markets.append(market)

    logger.info(f"Total unique markets discovered: {len(all_markets)}")
    return all_markets


def get_available_tags() -> List[Dict[str, Any]]:
    """
    Get all available tags from the API for display/filtering.

    Returns list of tags with id, label, and slug.
    """
    try:
        tags = fetch_all_tags()
        return [
            {
                "id": t.get("id"),
                "label": t.get("label") or t.get("name"),
                "slug": t.get("slug"),
            }
            for t in tags
        ]
    except Exception as e:
        logger.error(f"Error fetching tags: {e}")
        return []


def _build_game_id_mappings(markets: List[Dict[str, Any]]) -> int:
    """Build game_id -> market_id mappings for sports WS lookups."""
    mapped = 0
    for market in markets:
        game_id = market.get("game_id")
        market_id = market.get("market_id")
        if game_id and market_id:
            upsert_game_id_mapping(
                game_id=game_id,
                market_id=market_id,
                event_id=market.get("event_id"),
                game=market.get("game", "cod"),
            )
            mapped += 1
    if mapped:
        logger.info(f"Built {mapped} game_id mappings for sports WS")
    return mapped


def save_discovered_markets(markets: List[Dict[str, Any]]) -> int:
    """Save discovered markets to the database."""
    saved = 0
    for market_data in markets:
        if market_data.get("market_id"):
            upsert_market(market_data)
            saved += 1
    logger.info(f"Saved {saved} markets to database")
    return saved


def run_discovery(
    games: Optional[List[str]] = None,
    include_closed: bool = False,
    use_tags: bool = True,
) -> List[Dict[str, Any]]:
    """
    Run the full market discovery process.

    Args:
        games: List of game keys to discover. None means all games.
        include_closed: Include closed/resolved markets
        use_tags: Also use tag-based discovery (slower but more comprehensive)
    """
    if use_tags:
        markets = discover_all_markets(games=games, include_closed=include_closed)
    else:
        # Search-only, iterate per game
        if games is None:
            games = VALID_GAMES
        markets = []
        seen_ids: Set[str] = set()
        for game in games:
            for m in discover_game_markets(game, include_closed=include_closed):
                if m.get("market_id") not in seen_ids:
                    seen_ids.add(m["market_id"])
                    markets.append(m)

    save_discovered_markets(markets)
    _build_game_id_mappings(markets)
    return markets


def list_stored_markets(game: Optional[str] = None) -> List[Dict[str, Any]]:
    """List all markets stored in the database, optionally filtered by game."""
    return get_all_markets(game=game)
