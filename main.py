#!/usr/bin/env python3
"""
Graph AI Wallet Native Transfer Ingestion Service
Main entry point for the ingestion service that ingests native transfers.
"""

import sys
import json
import time
import os
import math
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from supabase import create_client, Client
import requests
from datetime import datetime, timedelta, timezone
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from logger import logger


# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://xfcduaalalfppjfoqwke.supabase.co")
SUPABASE_KEY = os.getenv(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhmY2R1YWFsYWxmcHBqZm9xd2tlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE4ODg3NjQsImV4cCI6MjA2NzQ2NDc2NH0.f5wyoVkiqO163JRPzjnPn9R3jN-gzqss1PZSJ6PRa2U",
)

# SNS Configuration
SNS_TOPIC_ARN = os.getenv(
    "SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:631447262747:BASE_CONTRACT"
)

# Allium API configuration
ALLIUM_API_KEY = os.getenv(
    "ALLIUM_API_KEY",
    "kijnI3_lzTVtPgJ32fe9fsysrs1iWakay-HRlm8EBIzxC43NjaIUjhqvPZGlrBkaQiBmGtFkNM1tzZPDNdtXdQ",
)
ALLIUM_BASE_URL = "https://api.allium.so/api/v1/developer/wallet/transactions"
ALLIUM_HEADERS = {"Content-Type": "application/json", "X-API-KEY": ALLIUM_API_KEY}

# Initialize SNS client
sns_client = boto3.client("sns", region_name="us-east-1")

# Table name for storing the last processed timestamp
TIMESTAMP_TABLE_NAME = "native_transfer_fetcher_pipeline_timestamp"


def parse_allium_timestamp(block_timestamp) -> Optional[datetime]:
    """
    Parse Allium API block_timestamp to datetime object.
    Allium returns timestamps in format: "2025-02-04T16:45:27" (ISO format without timezone).

    Args:
        block_timestamp: Timestamp from Allium API (string in ISO format)

    Returns:
        datetime object with UTC timezone, or None if parsing fails
    """
    if not block_timestamp:
        return None

    try:
        if isinstance(block_timestamp, str):
            # Allium format: "2025-02-04T16:45:27" (no timezone, assume UTC)
            # Parse as naive datetime and add UTC timezone
            dt = datetime.fromisoformat(block_timestamp)
            # If no timezone info, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        elif isinstance(block_timestamp, (int, float)):
            # Unix timestamp
            return datetime.fromtimestamp(block_timestamp, tz=timezone.utc)
        else:
            return None
    except Exception as e:
        logger.warning(
            "Error parsing Allium timestamp",
            extra={"error": str(e), "block_timestamp": block_timestamp},
        )
        return None


def get_last_processed_timestamp(supabase: Client) -> datetime:
    """
    Get the last processed timestamp from Supabase.
    Always returns a timestamp (should always exist in Supabase).

    Args:
        supabase: Supabase client instance

    Returns:
        datetime object representing the last processed timestamp

    Raises:
        Exception: If timestamp cannot be retrieved or parsed
    """
    try:
        response = (
            supabase.table(TIMESTAMP_TABLE_NAME).select("timestamp").limit(1).execute()
        )

        if response.data and len(response.data) > 0:
            timestamp_str = response.data[0].get("timestamp")
            if timestamp_str:
                # Parse ISO format timestamp
                return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

        # If no timestamp found, raise error (should always exist)
        raise ValueError(
            f"No timestamp found in {TIMESTAMP_TABLE_NAME}. Timestamp must always exist in Supabase."
        )
    except Exception as e:
        logger.error(
            "Error fetching last processed timestamp",
            extra={"error": str(e)},
        )
        raise


def update_last_processed_timestamp(supabase: Client, timestamp: datetime) -> None:
    """
    Update the last processed timestamp in Supabase.
    For a single-row table, deletes all rows and inserts the new timestamp.

    Args:
        supabase: Supabase client instance
        timestamp: datetime object representing the latest processed timestamp
    """
    try:
        # Convert to ISO format string with timezone
        timestamp_iso = timestamp.isoformat()

        # For single-row table: delete all existing records and insert new one
        # This ensures we always have exactly one record
        # Delete all rows (using a condition that matches all)
        supabase.table(TIMESTAMP_TABLE_NAME).delete().gte(
            "timestamp", "1970-01-01T00:00:00+00:00"
        ).execute()
        # Insert new record
        supabase.table(TIMESTAMP_TABLE_NAME).insert(
            {"timestamp": timestamp_iso}
        ).execute()
        logger.info(
            "Updated last processed timestamp", extra={"timestamp": timestamp_iso}
        )

    except Exception as e:
        logger.error(
            "Error updating last processed timestamp",
            extra={"error": str(e), "timestamp": timestamp.isoformat()},
        )
        raise


def get_all_wallets(supabase: Client) -> List[str]:
    """Fetch all wallet IDs from Supabase"""
    try:
        response = supabase.table("wallet").select("wallet_id").execute()
        wallets = [row["wallet_id"] for row in response.data]
        return wallets
    except Exception as e:
        logger.error("Error fetching wallets", extra={"error": str(e)})
        return []


def split_wallets_into_groups(
    wallets: List[str], max_groups: int = 9
) -> List[List[str]]:
    """
    Split wallets into groups for parallel API calls.
    Maximum of max_groups groups (default 9) to respect rate limits.

    Args:
        wallets: List of wallet addresses
        max_groups: Maximum number of groups (default: 9 for 9 calls/second limit)

    Returns:
        List of wallet groups
    """
    if not wallets:
        return []

    wallet_count = len(wallets)

    if wallet_count <= max_groups:
        # If we have fewer wallets than max_groups, create one group per wallet
        return [[wallet] for wallet in wallets]
    else:
        # If we have more wallets, divide into max_groups groups
        # Calculate wallets per group
        wallets_per_group = math.ceil(wallet_count / max_groups)
        groups = []

        for i in range(0, wallet_count, wallets_per_group):
            group = wallets[i : i + wallets_per_group]
            groups.append(group)

        return groups


def fetch_transactions_for_group(
    wallet_group: List[str],
    chain: str = "base",
    lookback_days: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch all transactions for a group of wallets.
    This function handles pagination internally.

    Args:
        wallet_group: List of wallet addresses in this group
        chain: Blockchain chain (default: "base")
        lookback_days: Optional number of days to look back

    Returns:
        List of all transactions for the wallets in this group
    """
    return get_all_wallet_transactions(wallet_group, chain, lookback_days)


def get_wallet_transactions_page(
    addresses: List[str],
    chain: str = "base",
    cursor: str = None,
    lookback_days: Optional[int] = None,
) -> Dict:
    """
    Fetch one page of wallet transactions from Allium API with unlimited retries and exponential backoff.

    Args:
        addresses: List of wallet addresses
        chain: Blockchain chain (default: "base")
        cursor: Pagination cursor
        lookback_days: Optional number of days to look back (default: None, fetches since genesis)
                       According to Allium API docs: https://docs.allium.so/api/developer/wallets/transactions#parameter-lookback-days
    """
    # Prepare request body with all addresses
    payload = [{"chain": chain, "address": addr} for addr in addresses]

    # Prepare query parameters
    params = {"limit": 1000}
    if cursor:
        params["cursor"] = cursor

    # Add lookback_days parameter if provided (for last day data)
    if lookback_days is not None:
        params["lookback_days"] = lookback_days

    # Exponential backoff configuration
    MAX_BACKOFF_SECONDS = 30
    INITIAL_DELAY = 1  # Start with 1 second
    attempt = 0
    current_delay = INITIAL_DELAY

    # Unlimited retries with exponential backoff
    while True:
        try:
            attempt += 1
            logger.info(
                "Making Allium API request",
                extra={
                    "attempt": attempt,
                    "address_count": len(addresses),
                    "has_cursor": cursor is not None,
                    "lookback_days": lookback_days,
                    "retry_delay": current_delay if attempt > 1 else 0,
                },
            )
            # Increase timeout for large requests (60 seconds for read, 10 seconds for connect)
            response = requests.post(
                ALLIUM_BASE_URL,
                headers=ALLIUM_HEADERS,
                params=params,
                json=payload,
                timeout=(10, 60),  # (connect_timeout, read_timeout)
            )

            logger.info(
                "Allium API request completed",
                extra={
                    "status_code": response.status_code,
                    "attempt": attempt,
                },
            )

            response.raise_for_status()
            return response.json()

        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            # Calculate exponential backoff delay (capped at MAX_BACKOFF_SECONDS)
            if attempt == 1:
                # First retry: use initial delay
                retry_delay = INITIAL_DELAY
            elif current_delay < MAX_BACKOFF_SECONDS:
                # Exponential backoff: double the delay, capped at MAX_BACKOFF_SECONDS
                current_delay = min(current_delay * 2, MAX_BACKOFF_SECONDS)
                retry_delay = current_delay
            else:
                # Already at max, continue with 30 seconds
                retry_delay = MAX_BACKOFF_SECONDS

            error_type = (
                "timeout"
                if isinstance(e, requests.exceptions.Timeout)
                else "request_error"
            )
            logger.warning(
                f"API request failed ({error_type}), retrying in {retry_delay} seconds...",
                extra={
                    "error": str(e),
                    "attempt": attempt,
                    "retry_delay": retry_delay,
                    "backoff_capped": current_delay >= MAX_BACKOFF_SECONDS,
                },
            )

            # Add response details if available
            if hasattr(e, "response") and e.response is not None:
                logger.debug(
                    "API error response details",
                    extra={
                        "response_status": e.response.status_code,
                        "response_preview": (
                            e.response.text[:200] if e.response.text else None
                        ),
                    },
                )

            time.sleep(retry_delay)
            # Continue to next iteration (unlimited retries)


def get_all_wallet_transactions(
    addresses: List[str], chain: str = "base", lookback_days: Optional[int] = None
) -> List[Dict]:
    """
    Fetch wallet transactions using pagination, filtered by lookback_days if provided.

    Args:
        addresses: List of wallet addresses
        chain: Blockchain chain (default: "base")
        lookback_days: Optional number of days to look back (default: None, fetches since genesis)
                       Set to 1 to fetch only last day data
    """
    all_transactions = []
    cursor = None
    page_count = 0

    logger.info(
        "Starting pagination loop for wallet transactions",
        extra={
            "address_count": len(addresses),
            "lookback_days": lookback_days,
        },
    )

    while True:
        page_count += 1

        logger.info(
            "Fetching transaction page",
            extra={
                "page": page_count,
                "has_cursor": cursor is not None,
                "address_count": len(addresses),
            },
        )

        # Get one page of results with lookback_days filtering
        results = get_wallet_transactions_page(addresses, chain, cursor, lookback_days)

        # Add transactions from this page to our collection
        if "items" in results and results["items"]:
            all_transactions.extend(results["items"])
            logger.info(
                "Received transaction page",
                extra={
                    "page": page_count,
                    "items_on_page": len(results["items"]),
                    "total_accumulated": len(all_transactions),
                },
            )
        else:
            logger.info(
                "No transactions found on this page", extra={"page": page_count}
            )
            break

        # Check if there's a cursor for the next page
        if "cursor" in results and results["cursor"]:
            cursor = results["cursor"]
            logger.debug(
                "Cursor received, continuing to next page", extra={"page": page_count}
            )
        else:
            logger.info("No more pages available", extra={"page": page_count})
            break

    logger.info(
        "Completed fetching transactions",
        extra={
            "total_transactions": len(all_transactions),
            "pages_fetched": page_count,
            "lookback_days": lookback_days,
        },
    )
    return all_transactions


def is_contract_address(address: str, wallets_set: Set[str]) -> Tuple[bool, str]:
    """
    Check if an address is a contract address using wallets from Supabase.
    If address is in wallets set, it's a wallet; otherwise, it's a contract.

    Args:
        address: The address to check (already has 0x prefix)
        wallets_set: Set of wallet addresses from Supabase (lowercase normalized)

    Returns:
        tuple: (is_contract: bool, wallet_address: str or empty string)
               If it's a contract, wallet_address is empty
               If it's a wallet, wallet_address contains the address
    """
    # Addresses are already normalized (0x prefix), just make lowercase
    normalized_addr = address.lower()

    # Check if address is in wallets set
    # If it's in the wallets set, it's a wallet; otherwise, treat as contract
    if normalized_addr in wallets_set:
        # It's a wallet
        wallet_address = address
        return False, wallet_address
    else:
        # Not in wallets set, treat as contract
        return True, ""


def batch_check_contracts(
    addresses: List[str], wallets_set: Set[str]
) -> Dict[str, Tuple[bool, str]]:
    """
    Check multiple addresses to see if they are contracts.

    Args:
        addresses: List of addresses to check
        wallets_set: Set of wallet addresses from Supabase (lowercase normalized)

    Returns:
        Dictionary mapping address to (is_contract: bool, wallet_address: str)
    """
    results = {}

    for addr in addresses:
        try:
            results[addr] = is_contract_address(addr, wallets_set)
        except Exception as e:
            logger.warning(
                "Error checking address", extra={"address": addr, "error": str(e)}
            )
            results[addr] = (True, "")  # Default to contract on error

    return results


def extract_native_transfers(
    transaction: Dict,
    api_request_time: str,
    contract_results: Dict[str, Tuple[bool, str]],
) -> tuple[Optional[Dict], Set[str]]:
    """Extract native transfers from a transaction and format them

    Args:
        transaction: Raw transaction dictionary from API
        api_request_time: ISO format timestamp when API request was made
        contract_results: Pre-computed results from batch contract checking

    Returns:
        tuple: (formatted_transaction, wallet_addresses_set)
    """
    native_transfers = []
    wallet_addresses = set()  # To track unique wallet addresses

    # Get asset transfers if they exist
    asset_transfers = transaction.get("asset_transfers", [])

    for transfer in asset_transfers:
        # Check if this is a native transfer
        asset = transfer.get("asset", {})
        if asset.get("type") == "native":
            from_address = transfer.get("from_address")
            to_address = transfer.get("to_address")

            # Look up contract results from batch check
            from_is_contract, from_wallet = contract_results.get(
                from_address, (True, "")
            )
            if not from_is_contract and from_wallet:
                wallet_addresses.add(from_wallet)

            to_is_contract, to_wallet = contract_results.get(to_address, (True, ""))
            if not to_is_contract and to_wallet:
                wallet_addresses.add(to_wallet)

            native_transfers.append(
                {
                    "from_address": from_address,
                    "to_address": to_address,
                    "amount": transfer.get("amount", {}).get("raw_amount"),
                }
            )

    # Only return formatted transaction if it has native transfers
    if native_transfers:
        formatted = {
            "api_request_time": api_request_time,
            "hash": transaction.get("hash"),
            "event_abi": "BASE_CONTRACT",
            "native_transfer": native_transfers,
            "block_timestamp": transaction.get("block_timestamp"),
            "block_number": transaction.get("block_number"),
        }
        return formatted, wallet_addresses

    return None, set()


def filter_and_transform_native_transfers(
    transactions: List[Dict],
    api_request_time: str,
    wallets_set: Set[str],
    last_processed_timestamp: datetime,
    upper_bound_timestamp: datetime,
) -> Tuple[List[tuple[Dict, Set[str]]], datetime]:
    """
    Filter and transform transactions to only include those with native transfers.
    Uses batch contract checking for performance.
    Only processes transactions between last_processed_timestamp (exclusive) and upper_bound_timestamp (inclusive).

    Args:
        transactions: List of raw transactions from API
        api_request_time: ISO format timestamp when API request was made
        wallets_set: Set of wallet addresses from Supabase (lowercase normalized)
        last_processed_timestamp: Timestamp to filter transactions (only process after this, exclusive)
        upper_bound_timestamp: Upper bound timestamp (only process up to this, inclusive)

    Returns:
        Tuple of (list of tuples: (formatted_transaction, wallet_addresses), latest_timestamp)
    """
    # Filter transactions by timestamp range
    filtered_transactions = []
    for transaction in transactions:
        block_timestamp = transaction.get("block_timestamp")
        tx_timestamp = parse_allium_timestamp(block_timestamp)

        if tx_timestamp:
            # Only include transactions after last_processed_timestamp and <= upper_bound_timestamp
            if (
                tx_timestamp > last_processed_timestamp
                and tx_timestamp <= upper_bound_timestamp
            ):
                filtered_transactions.append(transaction)
        else:
            # Include transactions without valid timestamp (to be safe)
            filtered_transactions.append(transaction)

    logger.info(
        "Filtered transactions by timestamp",
        extra={
            "original_count": len(transactions),
            "filtered_count": len(filtered_transactions),
            "last_processed_timestamp": last_processed_timestamp.isoformat(),
            "upper_bound_timestamp": upper_bound_timestamp.isoformat(),
        },
    )

    # First, collect all unique addresses from all filtered transactions
    unique_addresses = set()
    for transaction in filtered_transactions:
        asset_transfers = transaction.get("asset_transfers", [])
        for transfer in asset_transfers:
            asset = transfer.get("asset", {})
            if asset.get("type") == "native":
                from_address = transfer.get("from_address")
                to_address = transfer.get("to_address")
                if from_address:
                    unique_addresses.add(from_address)
                if to_address:
                    unique_addresses.add(to_address)

    # Batch check all addresses concurrently
    contract_results = batch_check_contracts(list(unique_addresses), wallets_set)

    # Now process transactions with pre-computed contract results
    native_transfer_transactions = []
    latest_timestamp = last_processed_timestamp

    for transaction in filtered_transactions:
        formatted, wallet_addresses = extract_native_transfers(
            transaction, api_request_time, contract_results
        )
        if formatted:
            native_transfer_transactions.append((formatted, wallet_addresses))

        # Update latest timestamp for ALL processed transactions (not just those with native transfers)
        # This ensures we don't reprocess the same transactions when no native transfers are found
        block_timestamp = transaction.get("block_timestamp")
        tx_timestamp = parse_allium_timestamp(block_timestamp)

        if tx_timestamp and tx_timestamp > latest_timestamp:
            latest_timestamp = tx_timestamp

    return native_transfer_transactions, latest_timestamp


def publish_to_sns(transaction: Dict, wallet_addresses: Set[str]) -> None:
    """Publish transaction to SNS topic with event_address array as message attributes"""
    try:
        # Prepare message
        message_body = json.dumps(transaction)

        # Prepare message attributes
        message_attributes = {}

        # Create event_address array with strings in format "NativeTransfer_<address>"
        event_address_list = [
            f"NativeTransfer_{address}" for address in wallet_addresses
        ]
        event_address_json = json.dumps(event_address_list)
        message_attributes["event_address"] = {
            "DataType": "String.Array",
            "StringValue": event_address_json,
        }

        # Publish to SNS using the hardcoded ARN
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message_body,
            MessageAttributes=message_attributes,
        )

    except Exception as e:
        logger.error(
            "Error publishing to SNS",
            extra={"error": str(e), "transaction_hash": transaction.get("hash")},
        )
        raise


def main():
    """Main function - Fetches wallet transactions every minute"""
    logger.info(
        "Graph AI Wallet Native Transfer Ingestion Service starting",
        extra={"python_version": sys.version},
    )

    # Initialize Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase client initialized")

    # Main service loop
    try:
        iteration = 0
        while True:
            loop_start_time = time.time()
            iteration += 1
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            timestamp_safe = current_time.replace(" ", "_").replace(":", "-")

            logger.info(
                "Starting iteration",
                extra={"iteration": iteration, "timestamp": current_time},
            )

            # Get current time at start of iteration (this will be the upper bound for this iteration)
            iteration_start_time = datetime.now(timezone.utc)
            api_request_time = iteration_start_time.isoformat()

            # Get last processed timestamp from Supabase (always required)
            last_processed_timestamp = get_last_processed_timestamp(supabase)
            logger.info(
                "Retrieved last processed timestamp",
                extra={
                    "timestamp": last_processed_timestamp.isoformat(),
                    "iteration_start_time": iteration_start_time.isoformat(),
                },
            )

            # Fetch all wallets from Supabase
            wallets = get_all_wallets(supabase)

            if not wallets:
                logger.warning("No wallets found in database. Skipping...")
                # Still update timestamp to iteration_start_time even with no wallets
                update_last_processed_timestamp(supabase, iteration_start_time)
                logger.info(
                    "Updated timestamp to iteration start time (no wallets)",
                    extra={"timestamp": iteration_start_time.isoformat()},
                )
            else:
                logger.info("Processing wallets", extra={"wallet_count": len(wallets)})

                # Convert wallets list to normalized set (lowercase) for efficient lookup
                wallets_set = {wallet.lower() for wallet in wallets}

                # Split wallets into groups (max 9 groups for rate limiting)
                wallet_groups = split_wallets_into_groups(wallets, max_groups=9)
                logger.info(
                    "Split wallets into groups for parallel API calls",
                    extra={
                        "total_wallets": len(wallets),
                        "number_of_groups": len(wallet_groups),
                        "wallets_per_group": [len(group) for group in wallet_groups],
                    },
                )

                # Fetch transactions for all groups in parallel (max 9 concurrent calls)
                logger.info(
                    "Starting to fetch transactions from Allium API (parallel groups)",
                    extra={
                        "group_count": len(wallet_groups),
                        "chain": "base",
                        "lookback_days": 1,
                    },
                )

                all_transactions = []
                with ThreadPoolExecutor(max_workers=9) as executor:
                    # Submit all group fetch tasks
                    futures = {
                        executor.submit(
                            fetch_transactions_for_group,
                            group,
                            "base",
                            1,  # lookback_days
                        ): group_idx
                        for group_idx, group in enumerate(wallet_groups)
                    }

                    # Collect results as they complete
                    for future in as_completed(futures):
                        group_idx = futures[future]
                        try:
                            group_transactions = future.result()
                            all_transactions.extend(group_transactions)
                            logger.info(
                                "Completed fetching transactions for group",
                                extra={
                                    "group_index": group_idx,
                                    "transactions_in_group": len(group_transactions),
                                    "total_accumulated": len(all_transactions),
                                },
                            )
                        except Exception as e:
                            logger.error(
                                "Error fetching transactions for group",
                                extra={
                                    "group_index": group_idx,
                                    "error": str(e),
                                    "error_type": type(e).__name__,
                                },
                            )

                logger.info(
                    "Finished fetching transactions from Allium API",
                    extra={"total_transactions": len(all_transactions)},
                )

                # Filter and transform to only include native transfers in the timestamp range
                # Range: last_processed_timestamp < tx_timestamp <= iteration_start_time
                if all_transactions:
                    native_transfer_transactions, latest_timestamp = (
                        filter_and_transform_native_transfers(
                            all_transactions,
                            api_request_time,
                            wallets_set,
                            last_processed_timestamp,
                            iteration_start_time,
                        )
                    )

                    # Publish each transaction to SNS in parallel
                    if native_transfer_transactions:
                        with ThreadPoolExecutor(max_workers=20) as executor:
                            futures = []
                            for (
                                transaction,
                                wallet_addresses,
                            ) in native_transfer_transactions:
                                future = executor.submit(
                                    publish_to_sns, transaction, wallet_addresses
                                )
                                futures.append(future)

                            # Wait for all publishes to complete
                            for future in as_completed(futures):
                                try:
                                    future.result()
                                except Exception as e:
                                    logger.error(
                                        "Error in parallel SNS publish",
                                        extra={"error": str(e)},
                                    )

                        logger.info(
                            "Successfully published all transactions to SNS",
                            extra={
                                "transaction_count": len(native_transfer_transactions)
                            },
                        )

                    # Update the last processed timestamp to iteration_start_time
                    # This is the time we captured at the start of the iteration
                    # This ensures we don't reprocess transactions from this time window
                    update_last_processed_timestamp(supabase, iteration_start_time)
                    logger.info(
                        "Updated last processed timestamp to iteration start time",
                        extra={
                            "timestamp": iteration_start_time.isoformat(),
                            "native_transfers_found": len(native_transfer_transactions)
                            > 0,
                            "transactions_processed": len(native_transfer_transactions),
                        },
                    )
                else:
                    # No transactions found, but still update timestamp to iteration_start_time
                    update_last_processed_timestamp(supabase, iteration_start_time)
                    logger.info(
                        "No transactions found, updated timestamp to iteration start time",
                        extra={
                            "timestamp": iteration_start_time.isoformat(),
                        },
                    )

            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time

            # Format duration for readability
            minutes = int(loop_duration // 60)
            seconds = int(loop_duration % 60)
            milliseconds = int((loop_duration % 1) * 1000)
            if minutes > 0:
                duration_formatted = f"{minutes}m {seconds}s {milliseconds}ms"
            else:
                duration_formatted = f"{seconds}s {milliseconds}ms"

            logger.info(
                "Iteration completed",
                extra={
                    "iteration": iteration,
                    "duration_seconds": round(loop_duration, 2),
                    "duration_formatted": duration_formatted,
                },
            )
            logger.info("Sleeping for 60 seconds before next iteration")
            time.sleep(60)  # Wait 60 seconds before next iteration

    except KeyboardInterrupt:
        logger.info("Shutting down service")
        logger.info("Service shutdown complete")


if __name__ == "__main__":
    main()
