from typing import Optional, Union
def write_json(bucket: str, key: str, data: Union[dict, list]) -> None:
    import boto3
    import json
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")

    # Convert to JSON string
    json_body = json.dumps(data)
    # Write to S3
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_body,
        ContentType="application/json"
    )
    print(f"Saved data to s3://{bucket}/{key}")

def read_json(bucket: str, key: str, default=None):

    """
    Read a JSON file from S3 and return the parsed object.
    Parameters
    ----------
    bucket : str
        S3 bucket name
    key : str
        S3 object key
    default : any, optional
        Value to return if the object does not exist
    Returns
    -------
    dict | list | any
        Parsed JSON object or default value
    """
    import boto3
    import json
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey":
            print(f"File not found: s3://{bucket}/{key}")
            return default
        raise

    except json.JSONDecodeError:
        print(f"Invalid JSON in s3://{bucket}/{key}")
        raise

def object_exists(bucket: str, key: str) -> bool:
    """
    Check whether an object exists in S3.
    Parameters
    ----------
    bucket : str
        S3 bucket name
    key : str
        S3 object key
    Returns
    -------
    bool
        True if object exists, otherwise False
    """
    import boto3
    import json
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ["404", "NoSuchKey"]:
            return False
        raise