#!/usr/bin/env python3
"""
Graph AI Wallet Native Transfer Ingestion Service
Main entry point for the ingestion service that ingests native transfers.
"""

import sys
import json
import time
import os
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


def get_last_processed_timestamp(supabase: Client) -> Optional[datetime]:
    """
    Get the last processed timestamp from Supabase.
    Returns None if no timestamp exists (first run).

    Args:
        supabase: Supabase client instance

    Returns:
        datetime object representing the last processed timestamp, or None
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

        return None
    except Exception as e:
        logger.warning(
            "Error fetching last processed timestamp, assuming first run",
            extra={"error": str(e)},
        )
        return None


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


def get_wallet_transactions_page(
    addresses: List[str],
    chain: str = "base",
    cursor: str = None,
    lookback_days: Optional[int] = None,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> Dict:
    """
    Fetch one page of wallet transactions from Allium API with retry logic.

    Args:
        addresses: List of wallet addresses
        chain: Blockchain chain (default: "base")
        cursor: Pagination cursor
        lookback_days: Optional number of days to look back (default: None, fetches since genesis)
                       According to Allium API docs: https://docs.allium.so/api/developer/wallets/transactions#parameter-lookback-days
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Delay in seconds between retries (default: 5)
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

    # Retry logic for handling timeouts and transient errors
    for attempt in range(max_retries):
        try:
            # Increase timeout for large requests (60 seconds for read, 10 seconds for connect)
            response = requests.post(
                ALLIUM_BASE_URL,
                headers=ALLIUM_HEADERS,
                params=params,
                json=payload,
                timeout=(10, 60),  # (connect_timeout, read_timeout)
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"API request timed out (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay} seconds...",
                    extra={"error": str(e), "attempt": attempt + 1},
                )
                time.sleep(retry_delay)
                continue
            else:
                extra_data = {"error": str(e), "attempt": attempt + 1}
                logger.error(
                    "API request timed out after all retries", extra=extra_data
                )
                raise

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"API request failed (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay} seconds...",
                    extra={"error": str(e), "attempt": attempt + 1},
                )
                time.sleep(retry_delay)
                continue
            else:
                extra_data = {"error": str(e)}
                if hasattr(e, "response") and e.response is not None:
                    extra_data["response_status"] = e.response.status_code
                    extra_data["response_content"] = e.response.text
                logger.error(
                    "Error making API request after all retries", extra=extra_data
                )
                raise


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

    while True:
        page_count += 1

        # Get one page of results with lookback_days filtering
        results = get_wallet_transactions_page(addresses, chain, cursor, lookback_days)

        # Add transactions from this page to our collection
        if "items" in results and results["items"]:
            all_transactions.extend(results["items"])
        else:
            logger.info(
                "No transactions found on this page", extra={"page": page_count}
            )
            break

        # Check if there's a cursor for the next page
        if "cursor" in results and results["cursor"]:
            cursor = results["cursor"]
        else:
            logger.info("No more pages available", extra={"page": page_count})
            break

    logger.info(
        "Completed fetching transactions",
        extra={
            "total_transactions": len(all_transactions),
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
    last_processed_timestamp: Optional[datetime] = None,
) -> Tuple[List[tuple[Dict, Set[str]]], Optional[datetime]]:
    """
    Filter and transform transactions to only include those with native transfers.
    Uses batch contract checking for performance.
    Only processes transactions after the last processed timestamp.

    Args:
        transactions: List of raw transactions from API
        api_request_time: ISO format timestamp when API request was made
        wallets_set: Set of wallet addresses from Supabase (lowercase normalized)
        last_processed_timestamp: Timestamp to filter transactions (only process after this)

    Returns:
        Tuple of (list of tuples: (formatted_transaction, wallet_addresses), latest_timestamp)
    """
    # Filter transactions by timestamp if last_processed_timestamp is provided
    filtered_transactions = []
    if last_processed_timestamp:
        for transaction in transactions:
            block_timestamp = transaction.get("block_timestamp")
            tx_timestamp = parse_allium_timestamp(block_timestamp)

            if tx_timestamp:
                # Only include transactions after the last processed timestamp
                if tx_timestamp > last_processed_timestamp:
                    filtered_transactions.append(transaction)
            else:
                # Include transactions without valid timestamp (to be safe)
                filtered_transactions.append(transaction)
    else:
        # No timestamp filter, process all transactions
        filtered_transactions = transactions

    logger.info(
        "Filtered transactions by timestamp",
        extra={
            "original_count": len(transactions),
            "filtered_count": len(filtered_transactions),
            "last_processed_timestamp": (
                last_processed_timestamp.isoformat()
                if last_processed_timestamp
                else None
            ),
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

        if tx_timestamp:
            if latest_timestamp is None or tx_timestamp > latest_timestamp:
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

            # Get API request time before fetching wallets
            api_request_time = datetime.now().isoformat()

            # Get last processed timestamp from Supabase
            last_processed_timestamp = get_last_processed_timestamp(supabase)
            if last_processed_timestamp:
                logger.info(
                    "Retrieved last processed timestamp",
                    extra={"timestamp": last_processed_timestamp.isoformat()},
                )
            else:
                logger.info("No previous timestamp found, processing from beginning")

            # Fetch all wallets from Supabase
            wallets = get_all_wallets(supabase)

            if not wallets:
                logger.warning("No wallets found in database. Skipping...")
            else:
                logger.info("Processing wallets", extra={"wallet_count": len(wallets)})

                # Convert wallets list to normalized set (lowercase) for efficient lookup
                wallets_set = {wallet.lower() for wallet in wallets}

                # Fetch transactions for last day only using lookback_days=1
                # According to Allium API: https://docs.allium.so/api/developer/wallets/transactions#parameter-lookback-days
                all_transactions = get_all_wallet_transactions(wallets, lookback_days=1)

                # Filter and transform to only include native transfers after last processed timestamp
                if all_transactions:
                    native_transfer_transactions, latest_timestamp = (
                        filter_and_transform_native_transfers(
                            all_transactions,
                            api_request_time,
                            wallets_set,
                            last_processed_timestamp,
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

                    # Update the last processed timestamp even if no native transfers were found
                    # This prevents reprocessing the same transactions in the next iteration
                    if (
                        latest_timestamp
                        and latest_timestamp != last_processed_timestamp
                    ):
                        update_last_processed_timestamp(supabase, latest_timestamp)
                        logger.info(
                            "Updated last processed timestamp",
                            extra={
                                "timestamp": latest_timestamp.isoformat(),
                                "native_transfers_found": len(
                                    native_transfer_transactions
                                )
                                > 0,
                            },
                        )
                    elif not native_transfer_transactions:
                        logger.info(
                            "No transactions with native transfers found",
                            extra={
                                "latest_timestamp": (
                                    latest_timestamp.isoformat()
                                    if latest_timestamp
                                    else None
                                ),
                                "last_processed_timestamp": (
                                    last_processed_timestamp.isoformat()
                                    if last_processed_timestamp
                                    else None
                                ),
                            },
                        )
                else:
                    logger.info("No transactions found")

            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            logger.info(
                "Loop processing complete",
                extra={
                    "duration_seconds": round(loop_duration, 2),
                    "iteration": iteration,
                },
            )
            logger.info("Sleeping for 1 minute before next iteration")
            time.sleep(60)  # Wait 1 minute before next iteration

    except KeyboardInterrupt:
        logger.info("Shutting down service")
        logger.info("Service shutdown complete")


if __name__ == "__main__":
    main()
