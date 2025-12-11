"""
Backfilling Service for GraphAI Native Transfers

This service processes backfilling requests by:
1. Getting the oldest pending request from backfilling_requests table
2. Fetching subgraph event subscriptions from database
3. Building name-address filter for Allium API query
4. Fetching historical events from Allium API
5. Sending events to Lambda in batches with backfilling_for_subgraph field
"""

import os
import sys
import time
import json
import requests
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from supabase import create_client, Client
from logger import logger

# Configuration from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ALLIUM_API_KEY = os.getenv("ALLIUM_API_KEY")

# Hardcoded configuration values
ALLIUM_QUERY_ID = "IZRFYycXqAakcbu0jNFy"
AWS_REGION = "us-east-1"
LAMBDA_FUNCTION_NAME = "GraphAIEventsIngestionService"
BATCH_SIZE = 100
POLLING_INTERVAL = 60  # seconds between request checks
ALLIUM_RUN_LIMIT = 1000  # records per Allium query

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)


def get_oldest_backfilling_request():
    """
    Get the oldest pending backfilling request from the database.

    Returns:
        dict: Backfilling request record or None if no pending requests
    """
    try:
        response = (
            supabase.table("backfilling_requests")
            .select("*")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .limit(1)
            .execute()
        )

        if response.data and len(response.data) > 0:
            return response.data[0]
        return None
    except Exception as e:
        logger.error("Failed to fetch backfilling request", extra={"error": str(e)})
        return None


def update_backfilling_status(request_id, status, error_message=None):
    """
    Update the status of a backfilling request.

    Args:
        request_id: ID of the backfilling request
        status: New status (processing/completed/failed)
        error_message: Optional error message for failed requests
    """
    try:
        update_data = {"status": status}
        if error_message:
            update_data["error_message"] = error_message

        supabase.table("backfilling_requests").update(update_data).eq(
            "id", request_id
        ).execute()

        logger.info(
            "Updated backfilling request status",
            extra={"request_id": request_id, "status": status},
        )
    except Exception as e:
        logger.error(
            "Failed to update backfilling status",
            extra={"request_id": request_id, "error": str(e)},
        )


def get_subgraph_subscribers(subgraph_id):
    """
    Get all event subscriptions for a subgraph.

    Args:
        subgraph_id: UUID of the subgraph

    Returns:
        list: List of subscriber records with event_type and contract_address
    """
    try:
        response = (
            supabase.table("subgraph_subscribers")
            .select("event_type, contract_address")
            .eq("subgraph_id", subgraph_id)
            .eq("status", "active")
            .execute()
        )

        return response.data
    except Exception as e:
        logger.error(
            "Failed to fetch subgraph subscribers",
            extra={"subgraph_id": subgraph_id, "error": str(e)},
        )
        return []


def build_name_address_filter(subscribers):
    """
    Build the name-address filter string for Allium API query.
    Pattern from allium_explorer.py lines 21-32

    Args:
        subscribers: List of dicts with event_type and contract_address

    Returns:
        str: Filter string like "(name='Transfer' and address='0x123') or ..."
    """
    if not subscribers:
        return ""

    # Build name-address pairs (lowercase addresses as per Allium convention)
    # Strip 'BASE_CONTRACT.' prefix from event_type if present
    name_address_pairs = []
    for sub in subscribers:
        event_type = sub["event_type"]
        # Remove 'BASE_CONTRACT.' prefix if present
        if event_type.startswith("BASE_CONTRACT."):
            event_type = event_type.replace("BASE_CONTRACT.", "")
        name_address_pairs.append((event_type, sub["contract_address"].lower()))

    # Build the filter string
    name_address_filter = " or ".join(
        f"(name='{name}' and address='{address}')"
        for name, address in name_address_pairs
    )

    logger.info(
        "Built name-address filter",
        extra={
            "total_pairs": len(name_address_pairs),
            "filter_preview": name_address_filter[:200],
        },
    )

    return name_address_filter


def run_allium_query(parameters):
    """
    Run an Allium query asynchronously and poll for results.
    Pattern from allium_explorer.py lines 62-131

    Args:
        parameters: Query parameters including time window and filter

    Returns:
        dict: Query results or None if failed
    """
    run_config = {"limit": ALLIUM_RUN_LIMIT}
    query_start_time = time.time()

    try:
        # Step 1: Start async query
        logger.info(
            "Starting Allium async query",
            extra={
                "query_id": ALLIUM_QUERY_ID,
                "time_range": f"{parameters.get('param_1')} to {parameters.get('param_10')}",
                "limit": ALLIUM_RUN_LIMIT,
            },
        )

        response = requests.post(
            f"https://api.allium.so/api/v1/explorer/queries/{ALLIUM_QUERY_ID}/run-async",
            json={"parameters": parameters, "run_config": run_config},
            headers={"X-API-KEY": ALLIUM_API_KEY},
        )

        response_data = response.json()

        if response.status_code != 200:
            logger.error(
                "Error starting Allium query",
                extra={
                    "status_code": response.status_code,
                    "response": response_data,
                },
            )
            return None

        run_id = response_data.get("run_id")
        if not run_id:
            logger.error(
                "No run_id in Allium response", extra={"response": response_data}
            )
            return None

        logger.info(
            "Allium query submitted successfully",
            extra={
                "run_id": run_id,
                "query_id": ALLIUM_QUERY_ID,
            },
        )

        # Step 2: Poll for completion
        max_attempts = 120  # 10 minutes with 5s intervals
        attempt = 0
        poll_start_time = time.time()
        last_status = None

        while attempt < max_attempts:
            attempt += 1
            time.sleep(5)

            response = requests.get(
                f"https://api.allium.so/api/v1/explorer/query-runs/{run_id}/status",
                headers={"X-API-KEY": ALLIUM_API_KEY},
            )

            status = response.text.strip('"')

            # Log status changes or every 10th attempt
            if status != last_status or attempt % 10 == 0:
                elapsed_time = time.time() - poll_start_time
                logger.info(
                    "Allium query status check",
                    extra={
                        "run_id": run_id,
                        "attempt": attempt,
                        "status": status,
                        "elapsed_seconds": round(elapsed_time, 1),
                        "max_attempts": max_attempts,
                    },
                )
                last_status = status

            if status == "success":
                poll_duration = time.time() - poll_start_time
                logger.info(
                    "Allium query completed successfully",
                    extra={
                        "run_id": run_id,
                        "total_attempts": attempt,
                        "poll_duration_seconds": round(poll_duration, 1),
                    },
                )
                break
            elif status == "failed":
                logger.error(
                    "Allium query failed",
                    extra={
                        "run_id": run_id,
                        "attempt": attempt,
                        "status": status,
                    },
                )
                return None
            elif status not in ["created", "queued", "running"]:
                logger.warning(
                    "Unknown Allium query status",
                    extra={
                        "run_id": run_id,
                        "status": status,
                        "attempt": attempt,
                    },
                )

        if attempt >= max_attempts:
            timeout_duration = time.time() - poll_start_time
            logger.error(
                "Allium query timed out",
                extra={
                    "run_id": run_id,
                    "max_attempts": max_attempts,
                    "timeout_seconds": round(timeout_duration, 1),
                    "last_status": last_status,
                },
            )
            return None

        # Step 3: Get results
        logger.info("Fetching Allium query results", extra={"run_id": run_id})

        fetch_start_time = time.time()
        response = requests.get(
            f"https://api.allium.so/api/v1/explorer/query-runs/{run_id}/results",
            headers={"X-API-KEY": ALLIUM_API_KEY},
        )

        if response.status_code != 200:
            logger.error(
                "Error fetching Allium results",
                extra={
                    "run_id": run_id,
                    "status_code": response.status_code,
                    "response_text": response.text[:500],
                },
            )
            return None

        results = response.json()
        fetch_duration = time.time() - fetch_start_time
        total_duration = time.time() - query_start_time

        num_records = len(results.get("data", []))
        logger.info(
            "Allium query results fetched",
            extra={
                "run_id": run_id,
                "num_records": num_records,
                "fetch_duration_seconds": round(fetch_duration, 1),
                "total_duration_seconds": round(total_duration, 1),
            },
        )

        return results

    except Exception as e:
        logger.error(
            "Exception in Allium query",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": parameters,
            },
        )
        return None


def fetch_allium_events(start_time, end_time, name_address_filter):
    """
    Fetch all events from Allium API for the given time window with pagination.
    Pattern from allium_explorer.py lines 133-199

    Args:
        start_time: Start timestamp (ISO format)
        end_time: End timestamp (ISO format)
        name_address_filter: Filter string for events

    Returns:
        list: All fetched event records
    """
    all_data = []
    batch_num = 1
    fetch_start_time = time.time()

    # Initial parameters
    parameters = {
        "param_1": start_time,
        "param_10": end_time,
        "param_name_address_filter": name_address_filter,
    }

    logger.info(
        "Starting Allium events fetch",
        extra={
            "start_time": start_time,
            "end_time": end_time,
            "filter_length": len(name_address_filter),
        },
    )

    while True:
        batch_start_time = time.time()

        logger.info(
            f"Fetching Allium batch {batch_num}",
            extra={
                "batch_num": batch_num,
                "start": parameters["param_1"],
                "end": parameters["param_10"],
                "total_records_so_far": len(all_data),
            },
        )

        results = run_allium_query(parameters)

        if results is None:
            logger.error(
                "Allium query failed, stopping pagination",
                extra={
                    "batch_num": batch_num,
                    "records_collected_so_far": len(all_data),
                },
            )
            break

        batch_data = results.get("data", [])
        num_records = len(batch_data)
        batch_duration = time.time() - batch_start_time

        logger.info(
            f"Retrieved Allium batch {batch_num}",
            extra={
                "batch_num": batch_num,
                "records_in_batch": num_records,
                "total_records": len(all_data) + num_records,
                "batch_duration_seconds": round(batch_duration, 1),
            },
        )

        all_data.extend(batch_data)

        # Check if we hit the limit (meaning there might be more data)
        if num_records >= ALLIUM_RUN_LIMIT:
            logger.info(
                "Hit Allium limit, fetching next batch",
                extra={
                    "batch_num": batch_num,
                    "limit": ALLIUM_RUN_LIMIT,
                    "total_records_so_far": len(all_data),
                },
            )

            # Get the timestamp of the last record for next query
            last_record = batch_data[-1]
            last_timestamp = last_record.get("block_timestamp")

            if not last_timestamp:
                logger.warning(
                    "No block_timestamp in last record, stopping pagination",
                    extra={
                        "batch_num": batch_num,
                        "last_record_keys": (
                            list(last_record.keys()) if last_record else []
                        ),
                    },
                )
                break

            logger.info(
                "Continuing pagination with new timestamp",
                extra={
                    "batch_num": batch_num,
                    "next_batch": batch_num + 1,
                    "new_end_timestamp": last_timestamp,
                },
            )

            # Update param_10 to the last timestamp for the next query
            parameters["param_10"] = last_timestamp
            batch_num += 1
        else:
            total_duration = time.time() - fetch_start_time
            logger.info(
                "All Allium data retrieved successfully",
                extra={
                    "total_batches": batch_num,
                    "total_records": len(all_data),
                    "total_duration_seconds": round(total_duration, 1),
                    "average_batch_time": round(total_duration / batch_num, 1),
                },
            )
            break

    return all_data


def invoke_lambda_with_batch(batch_data, subgraph_id):
    """
    Invoke Lambda function with a batch of logs.
    Pattern from consumer.py lines 64-84

    Args:
        batch_data: List of event logs to send
        subgraph_id: UUID of the subgraph being backfilled

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Add backfilling_for_subgraph at top level as specified
        payload = {"logs": batch_data, "backfilling_for_subgraph": subgraph_id}

        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType="Event",  # Async invocation
            Payload=json.dumps(payload),
        )

        # Check response status
        status_code = response.get("StatusCode")
        function_error = response.get("FunctionError")

        if function_error:
            logger.error(
                "Lambda function error",
                extra={
                    "error": function_error,
                    "batch_size": len(batch_data),
                    "subgraph_id": subgraph_id,
                    "status_code": status_code,
                },
            )
            return False

        # For async invocations, StatusCode should be 202 (Accepted)
        if status_code != 202:
            logger.error(
                "Lambda invocation returned unexpected status code",
                extra={
                    "status_code": status_code,
                    "expected": 202,
                    "batch_size": len(batch_data),
                    "subgraph_id": subgraph_id,
                },
            )
            return False

        logger.debug(
            "Lambda invoked successfully",
            extra={
                "batch_size": len(batch_data),
                "subgraph_id": subgraph_id,
                "status_code": status_code,
            },
        )

        return True

    except ClientError as e:
        logger.error(
            "Lambda invocation failed (ClientError)",
            extra={
                "error": str(e),
                "error_code": e.response.get("Error", {}).get("Code"),
                "batch_size": len(batch_data),
                "subgraph_id": subgraph_id,
            },
        )
        return False
    except Exception as e:
        logger.error(
            "Unexpected error during Lambda invocation",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "batch_size": len(batch_data),
                "subgraph_id": subgraph_id,
            },
        )
        return False


def send_events_to_lambda(events, subgraph_id):
    """
    Send events to Lambda in batches.
    Pattern from consumer.py lines 112-125

    Args:
        events: List of all events to send
        subgraph_id: UUID of the subgraph being backfilled

    Returns:
        tuple: (total_sent, total_failed)
    """
    total_events = len(events)
    total_sent = 0
    total_failed = 0
    total_batches = (total_events + BATCH_SIZE - 1) // BATCH_SIZE
    send_start_time = time.time()

    logger.info(
        "Starting to send events to Lambda",
        extra={
            "total_events": total_events,
            "batch_size": BATCH_SIZE,
            "total_batches": total_batches,
            "subgraph_id": subgraph_id,
        },
    )

    # Process in batches
    for batch_idx, i in enumerate(range(0, total_events, BATCH_SIZE), start=1):
        batch = events[i : i + BATCH_SIZE]
        batch_start_time = time.time()

        if invoke_lambda_with_batch(batch, subgraph_id):
            total_sent += len(batch)
            batch_duration = time.time() - batch_start_time

            # Log progress every 10 batches or on first/last batch
            if batch_idx == 1 or batch_idx == total_batches or batch_idx % 10 == 0:
                elapsed_time = time.time() - send_start_time
                progress_pct = (batch_idx / total_batches) * 100
                logger.info(
                    f"Lambda batch progress: {batch_idx}/{total_batches}",
                    extra={
                        "batch_num": batch_idx,
                        "total_batches": total_batches,
                        "progress_percent": round(progress_pct, 1),
                        "events_sent": total_sent,
                        "batch_duration_seconds": round(batch_duration, 2),
                        "elapsed_seconds": round(elapsed_time, 1),
                    },
                )
        else:
            total_failed += len(batch)
            logger.error(
                f"Lambda batch {batch_idx} failed",
                extra={
                    "batch_num": batch_idx,
                    "batch_size": len(batch),
                    "total_failed_so_far": total_failed,
                },
            )

    total_duration = time.time() - send_start_time
    logger.info(
        "Completed sending events to Lambda",
        extra={
            "total_events": total_events,
            "sent": total_sent,
            "failed": total_failed,
            "batches": total_batches,
            "total_duration_seconds": round(total_duration, 1),
            "average_batch_time": (
                round(total_duration / total_batches, 2) if total_batches > 0 else 0
            ),
        },
    )

    return total_sent, total_failed


def process_backfilling_request(request):
    """
    Process a single backfilling request end-to-end.

    Args:
        request: Backfilling request record from database

    Returns:
        bool: True if successful, False otherwise
    """
    request_id = request["id"]
    subgraph_id = request["subgraph_id"]
    process_start_time = time.time()

    # Handle both old schema (backfilling_window JSONB) and new schema (start_time/end_time columns)
    if "backfilling_window" in request and request["backfilling_window"]:
        # Old schema: JSONB field
        backfilling_window = request["backfilling_window"]
        start_time = backfilling_window.get("start_time")
        end_time = backfilling_window.get("end_time")
    else:
        # New schema: separate columns
        start_time = request.get("start_time")
        end_time = request.get("end_time")

    logger.info(
        "=" * 80,
    )
    logger.info(
        "Starting backfilling request processing",
        extra={
            "request_id": request_id,
            "subgraph_id": subgraph_id,
            "start_time": start_time,
            "end_time": end_time,
            "status": request.get("status"),
        },
    )

    try:
        # Validate time window
        if not start_time or not end_time:
            logger.error(
                "Invalid time window",
                extra={"start_time": start_time, "end_time": end_time},
            )
            update_backfilling_status(
                request_id, "failed", "Missing start_time or end_time"
            )
            return False

        # Mark as processing
        logger.info("Updating status to 'processing'", extra={"request_id": request_id})
        update_backfilling_status(request_id, "processing")

        # Get subgraph event subscriptions
        logger.info("Fetching subgraph subscribers", extra={"subgraph_id": subgraph_id})
        subscribers = get_subgraph_subscribers(subgraph_id)

        if not subscribers:
            logger.warning(
                "No active subscribers for subgraph", extra={"subgraph_id": subgraph_id}
            )
            update_backfilling_status(
                request_id, "failed", "No active event subscribers"
            )
            return False

        logger.info(
            "Found subgraph subscribers",
            extra={
                "count": len(subscribers),
                "subscriber_summary": [
                    f"{s['event_type']}@{s['contract_address'][:10]}..."
                    for s in subscribers[:5]
                ],
                "total_subscribers": len(subscribers),
            },
        )

        # Build name-address filter
        logger.info("Building Allium query filter")
        name_address_filter = build_name_address_filter(subscribers)

        if not name_address_filter:
            logger.error("Failed to build name-address filter")
            update_backfilling_status(request_id, "failed", "Empty filter")
            return False

        # Convert timestamps to ISO format string if they're not already
        start_time_str = str(start_time) if start_time else None
        end_time_str = str(end_time) if end_time else None

        # Fetch events from Allium
        logger.info(
            "Phase 1: Fetching events from Allium",
            extra={
                "start": start_time_str,
                "end": end_time_str,
                "filter_length": len(name_address_filter),
            },
        )

        events = fetch_allium_events(start_time_str, end_time_str, name_address_filter)

        if not events:
            logger.warning(
                "No events found in time window",
                extra={
                    "start": start_time_str,
                    "end": end_time_str,
                },
            )
            # Mark as completed even with no events (valid result)
            update_backfilling_status(request_id, "completed")
            logger.info("Request completed with 0 events")
            return True

        logger.info(
            "Phase 1 complete: Events fetched from Allium",
            extra={
                "total_events": len(events),
                "first_event_timestamp": (
                    events[0].get("block_timestamp") if events else None
                ),
                "last_event_timestamp": (
                    events[-1].get("block_timestamp") if events else None
                ),
            },
        )

        # Send events to Lambda
        logger.info(
            "Phase 2: Sending events to Lambda",
            extra={"total_events": len(events)},
        )
        total_sent, total_failed = send_events_to_lambda(events, subgraph_id)

        if total_failed > 0:
            logger.warning(
                "Some events failed to send to Lambda",
                extra={
                    "failed": total_failed,
                    "sent": total_sent,
                    "failure_rate": f"{(total_failed / len(events) * 100):.1f}%",
                },
            )
            update_backfilling_status(
                request_id,
                "failed",
                f"Failed to send {total_failed}/{len(events)} events",
            )
            return False

        # Mark as completed
        update_backfilling_status(request_id, "completed")

        total_duration = time.time() - process_start_time
        logger.info(
            "Backfilling request completed successfully",
            extra={
                "request_id": request_id,
                "events_processed": total_sent,
                "total_duration_seconds": round(total_duration, 1),
                "events_per_second": (
                    round(total_sent / total_duration, 2) if total_duration > 0 else 0
                ),
            },
        )
        logger.info(
            "=" * 80,
        )

        return True

    except Exception as e:
        total_duration = time.time() - process_start_time
        logger.error(
            "Exception processing backfilling request",
            extra={
                "request_id": request_id,
                "error": str(e),
                "error_type": type(e).__name__,
                "duration_before_failure": round(total_duration, 1),
            },
        )
        update_backfilling_status(request_id, "failed", str(e))
        return False


def main():
    """
    Main service loop that continuously processes backfilling requests.
    """
    logger.info(
        "Backfilling service started",
        extra={"polling_interval": POLLING_INTERVAL, "batch_size": BATCH_SIZE},
    )

    # Validate configuration
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.critical("Missing Supabase configuration")
        sys.exit(1)

    if not ALLIUM_API_KEY or not ALLIUM_QUERY_ID:
        logger.critical("Missing Allium configuration")
        sys.exit(1)

    logger.info("Configuration validated")

    while True:
        try:
            # Get oldest pending request
            request = get_oldest_backfilling_request()

            if request:
                logger.info(
                    "Found pending backfilling request",
                    extra={"request_id": request["id"]},
                )
                process_backfilling_request(request)
            else:
                logger.debug("No pending backfilling requests")

            # Wait before checking again
            time.sleep(POLLING_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            break
        except Exception as e:
            logger.error("Unexpected error in main loop", extra={"error": str(e)})
            time.sleep(POLLING_INTERVAL)

    logger.info("Backfilling service stopped")


if __name__ == "__main__":
    main()
