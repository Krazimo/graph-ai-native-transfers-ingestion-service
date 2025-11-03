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
from datetime import datetime
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed


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


def get_all_wallets(supabase: Client) -> List[str]:
    """Fetch all wallet IDs from Supabase"""
    try:
        response = supabase.table("wallet").select("wallet_id").execute()
        wallets = [row["wallet_id"] for row in response.data]
        return wallets
    except Exception as e:
        print(f"❌ Error fetching wallets: {e}")
        return []


def get_wallet_transactions_page(
    addresses: List[str], chain: str = "base", cursor: str = None
) -> Dict:
    """Fetch one page of wallet transactions from Allium API"""

    # Prepare request body with all addresses
    payload = [{"chain": chain, "address": addr} for addr in addresses]

    # Prepare query parameters
    params = {"limit": 1000}
    if cursor:
        params["cursor"] = cursor

    try:
        response = requests.post(
            ALLIUM_BASE_URL,
            headers=ALLIUM_HEADERS,
            params=params,
            json=payload,
            timeout=30,
        )

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ Error making API request: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        raise


def get_all_wallet_transactions(
    addresses: List[str], chain: str = "base"
) -> List[Dict]:
    """Fetch ALL wallet transactions using pagination"""
    all_transactions = []
    cursor = None
    page_count = 0

    while True:
        page_count += 1

        # Get one page of results
        results = get_wallet_transactions_page(addresses, chain, cursor)

        # Add transactions from this page to our collection
        if "items" in results and results["items"]:
            all_transactions.extend(results["items"])
        else:
            print(f"  ℹ️ No transactions found on this page")
            break

        # Check if there's a cursor for the next page
        if "cursor" in results and results["cursor"]:
            cursor = results["cursor"]
        else:
            print(f"  ✓ No more pages available")
            break

    print(f"\n🎉 Completed! Total transactions fetched: {len(all_transactions)}")
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
            print(f"⚠️ Error checking address {addr}: {e}")
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
    transactions: List[Dict], api_request_time: str, wallets_set: Set[str]
) -> List[tuple[Dict, Set[str]]]:
    """Filter and transform transactions to only include those with native transfers.
    Uses batch contract checking for performance.

    Args:
        transactions: List of raw transactions from API
        api_request_time: ISO format timestamp when API request was made
        wallets_set: Set of wallet addresses from Supabase (lowercase normalized)

    Returns:
        List of tuples: (formatted_transaction, wallet_addresses)
    """
    # First, collect all unique addresses from all transactions
    unique_addresses = set()
    for transaction in transactions:
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
    for transaction in transactions:
        formatted, wallet_addresses = extract_native_transfers(
            transaction, api_request_time, contract_results
        )
        if formatted:
            native_transfer_transactions.append((formatted, wallet_addresses))

    return native_transfer_transactions


def publish_to_sns(transaction: Dict, wallet_addresses: Set[str]) -> None:
    """Publish transaction to SNS topic with wallet addresses as message attributes"""
    try:
        # Prepare message
        message_body = json.dumps(transaction)

        # Prepare message attributes
        message_attributes = {}

        # Add wallet addresses as message attribute
        wallet_addresses_list = list(wallet_addresses)
        wallet_addresses_json = json.dumps(wallet_addresses_list)
        message_attributes["wallet_addresses"] = {
            "DataType": "String.Array",
            "StringValue": wallet_addresses_json,
        }

        # Publish to SNS using the hardcoded ARN
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message_body,
            MessageAttributes=message_attributes,
        )

    except Exception as e:
        print(f"❌ Error publishing to SNS: {e}")
        raise


def main():
    """Main function - Fetches wallet transactions every minute"""
    print("🚀 Graph AI Wallet Native Transfer Ingestion Service")
    print("=" * 60)
    print(f"Python version: {sys.version}")

    # Initialize Supabase client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Supabase client initialized")

    # Main service loop
    try:
        iteration = 0
        while True:
            loop_start_time = time.time()
            iteration += 1
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            timestamp_safe = current_time.replace(" ", "_").replace(":", "-")

            print(f"\n{'='*60}")
            print(f"⏰ Iteration #{iteration} - {current_time}")
            print(f"{'='*60}")

            # Get API request time before fetching wallets
            api_request_time = datetime.now().isoformat()

            # Fetch all wallets from Supabase
            wallets = get_all_wallets(supabase)

            if not wallets:
                print("⚠️ No wallets found in database. Skipping...")
            else:
                print(f"🔍 Processing {len(wallets)} wallet(s)")

                # Convert wallets list to normalized set (lowercase) for efficient lookup
                wallets_set = {wallet.lower() for wallet in wallets}

                # Fetch all transactions for these wallets
                all_transactions = get_all_wallet_transactions(wallets)

                # Filter and transform to only include native transfers
                if all_transactions:
                    native_transfer_transactions = (
                        filter_and_transform_native_transfers(
                            all_transactions, api_request_time, wallets_set
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
                                    print(f"❌ Error in parallel SNS publish: {e}")

                        print(
                            f"✅ Successfully published all {len(native_transfer_transactions)} transactions to SNS"
                        )
                    else:
                        print("ℹ️ No transactions with native transfers found, skipping")
                else:
                    print("ℹ️ No transactions found")

            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            print(f"\n⏱️ Loop processing time: {loop_duration:.2f} seconds")
            print(f"💤 Sleeping for 1 minute...")
            time.sleep(60)  # Wait 1 minute before next iteration

    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down...")
        print("✅ Service shutdown complete")


if __name__ == "__main__":
    main()
