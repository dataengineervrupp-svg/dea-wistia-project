from datetime import datetime, timezone

def build_manifest(
    run_id: str,
    endpoint: str,
    record_count: int,
    status: str,
    s3_key: str,
    metadata: dict | None = None
) -> dict:
    """
    Build a manifest record for pipeline auditing.

    Parameters
    ----------
    run_id : str
        Unique pipeline execution ID
    endpoint : str
        API endpoint or entity name (media, events, visitors)
    record_count : int
        Number of records written
    status : str
        Pipeline status (success, failed, partial_success)
    s3_key : str
        S3 location where data was written
    metadata : dict, optional
        Additional pipeline metadata

    Returns
    -------
    dict
        Manifest dictionary
    """

    manifest = {
        "run_id": run_id,
        "endpoint": endpoint,
        "record_count": record_count,
        "status": status,
        "s3_key": s3_key,
        "created_at_utc": datetime.now(timezone.utc).isoformat()
    }

    if metadata:
        manifest["metadata"] = metadata

    return manifest