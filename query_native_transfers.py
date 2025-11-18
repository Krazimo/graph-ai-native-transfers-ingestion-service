#!/usr/bin/env python3
"""
Script to query native transfers for specific wallets in the last 1 day.
"""

import json
import sys
from datetime import datetime, timezone
from typing import List, Dict
from main import (
    get_wallet_transactions_page,
    get_all_wallet_transactions,
    parse_allium_timestamp,
    extract_native_transfers,
    is_contract_address,
    batch_check_contracts,
)
from logger import logger


def query_native_transfers_for_wallets(wallet_addresses: List[str]) -> List[Dict]:
    """
    Query native transfers for the given wallet addresses in the last 1 day.
    
    Args:
        wallet_addresses: List of wallet addresses to query
        
    Returns:
        List of transactions with native transfers
    """
    logger.info(
        "Querying native transfers for wallets",
        extra={"wallet_count": len(wallet_addresses), "wallets": wallet_addresses}
    )
    
    # Fetch all transactions for last 1 day
    all_transactions = get_all_wallet_transactions(
        wallet_addresses, 
        chain="base", 
        lookback_days=1
    )
    
    logger.info(
        "Fetched transactions",
        extra={"total_transactions": len(all_transactions)}
    )
    
    # Create a set of wallet addresses for contract checking
    # Since we're querying for these specific wallets, they are all wallets
    wallets_set = {addr.lower() for addr in wallet_addresses}
    
    # Collect all unique addresses from transactions to check if they're contracts
    unique_addresses = set()
    for transaction in all_transactions:
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
    
    # Batch check all addresses
    contract_results = batch_check_contracts(list(unique_addresses), wallets_set)
    
    # Extract native transfers from transactions
    api_request_time = datetime.now().isoformat()
    native_transfer_transactions = []
    
    for transaction in all_transactions:
        formatted, wallet_addresses_set = extract_native_transfers(
            transaction, api_request_time, contract_results
        )
        if formatted:
            native_transfer_transactions.append({
                "transaction": formatted,
                "wallet_addresses": list(wallet_addresses_set)
            })
    
    logger.info(
        "Found native transfer transactions",
        extra={"native_transfer_count": len(native_transfer_transactions)}
    )
    
    return native_transfer_transactions


def format_output(results: List[Dict]) -> str:
    """
    Format the results for display.
    
    Args:
        results: List of native transfer transactions
        
    Returns:
        Formatted string output
    """
    if not results:
        return "No native transfers found in the last 1 day.\n"
    
    output = []
    output.append(f"Found {len(results)} transaction(s) with native transfers in the last 1 day:\n")
    output.append("=" * 80 + "\n")
    
    for idx, result in enumerate(results, 1):
        tx = result["transaction"]
        wallet_addresses = result["wallet_addresses"]
        
        output.append(f"\nTransaction #{idx}:")
        output.append(f"  Hash: {tx.get('hash')}")
        output.append(f"  Block Number: {tx.get('block_number')}")
        output.append(f"  Block Timestamp: {tx.get('block_timestamp')}")
        output.append(f"  Wallet Addresses Involved: {', '.join(wallet_addresses) if wallet_addresses else 'None'}")
        output.append(f"  Native Transfers:")
        
        for transfer in tx.get("native_transfer", []):
            from_addr = transfer.get("from_address", "")
            to_addr = transfer.get("to_address", "")
            amount = transfer.get("amount", "0")
            # Convert wei to ETH (1 ETH = 10^18 wei)
            try:
                amount_eth = int(amount) / 1e18 if amount else 0
                output.append(f"    From: {from_addr}")
                output.append(f"    To: {to_addr}")
                output.append(f"    Amount: {amount} wei ({amount_eth:.6f} ETH)")
            except (ValueError, TypeError):
                output.append(f"    From: {from_addr}")
                output.append(f"    To: {to_addr}")
                output.append(f"    Amount: {amount} wei")
        
        output.append("")
    
    return "\n".join(output)


def main():
    """Main function to query native transfers"""
    # Wallet addresses to query
    wallet_addresses = [
        '0x56da5eaeb257a822da3283ab074d8a1ce2c5e3f5',
        '0x60acb9ab1dca91019d9d428552135cc001db75d1',
        '0x4475e2a824a1e38d17a5dce389b9ab5df3de186f',
        '0x5cdfc27438fac9499e1e78f546ea668243280f87',
        '0xe174131ce7c17ecad6724a6e531a1c2798002d82',
        '0xcd268a24ad3cb746118276829a8367ba91c8bd89',
        '0x6cebe3b21f4b78893655a919c276ff0c0aa91e54'
    ]
    
    try:
        # Query native transfers
        results = query_native_transfers_for_wallets(wallet_addresses)
        
        # Format and print output
        output = format_output(results)
        print(output)
        
        # Also save to JSON file for programmatic access
        output_file = "native_transfers_last_day.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(
            "Results saved to file",
            extra={"output_file": output_file, "result_count": len(results)}
        )
        print(f"\nResults also saved to: {output_file}")
        
    except Exception as e:
        logger.error(
            "Error querying native transfers",
            extra={"error": str(e), "error_type": type(e).__name__}
        )
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()



