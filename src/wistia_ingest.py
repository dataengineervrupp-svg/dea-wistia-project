from wistia.api_client import WistiaClient
from wistia.s3_io import write_json, read_json, object_exists
from wistia.manifest import build_manifest
# from wistia.state import read_state, write_state

from wistia.config import BUCKET_NAME, BRONZE_EVENTS_PREFIX, BRONZE_MEDIA_PREFIX, BRONZE_VISITORS_PREFIX

'''
Project Flow

Get all media
    current run_id structure is fine
    but add check that if the run_id folder already exists, get the media_ids from the media.json

For each media_id:
    Ping event media manifest to see what was the most recent date for which events were saved
    For each date between most-recently-retrieved and yesterday:
        Get events page-by-page
        Save each page as events/media_id=.../page_n.json

Don't need visitors at all since dim_visitor comes from events

Silver events will append only dates that are beyond its current range

'''

def main():
    import pandas as pd
    from datetime import datetime, timedelta, timezone
    client = WistiaClient()
    
    # GET AND WRITE MEDIA
    media = client.list_media()
    media_s3_key = f"{BRONZE_MEDIA_PREFIX}/run_id={client.run_id}/media.json"
    write_json(BUCKET_NAME, media_s3_key, media)

    # WRITE MEDIA MANIFEST
    media_manifest_s3_key = f"manifests/media/{client.run_id}.json"
    media_manifest = build_manifest(
        run_id = client.run_id,
        endpoint = "media",
        record_count = len(media),
        status = "success",
        s3_key = f"{BUCKET_NAME}/{media_s3_key}",
        metadata = {'build_type':'test'}
    )
    write_json(BUCKET_NAME, media_manifest_s3_key, media_manifest)

    # MEDIA ID SET
    media_id_set = set([m['hashed_id'] for m in media])

    # GET EVENTS DATES FROM SILVER TABLE
    SILVER_EVENTS_PATH = f"s3://{BUCKET_NAME}/silver/events/"
    STATE_KEY = "state/events_state.json"
    
    end_date = datetime.date(datetime.now()) - timedelta(days=1)
    if not object_exists(BUCKET_NAME, STATE_KEY) or not object_exists(BUCKET_NAME, SILVER_EVENTS_PATH):
        start_date = '1900-01-01'
        # start_date = '2026-05-01'
    else:
        df_events = pd.read_parquet(SILVER_EVENTS_PATH)
        eds = df_events['event_date'].astype('datetime64[ns]')
        eds2 = eds.apply(lambda x: datetime.date(x))
        start_date = eds2.max() + timedelta(days=1)
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
    print('getting events from', start_date, 'to', end_date)
    # GET AND WRITE EVENTS WHILE SAVING UNIQUE VISITOR IDs
    visitor_id_set = set()
    event_manifest_full = []
    earliest_event_date = None
    most_recent_event_date = None
    event_record_count = 0
    medias_to_get = len(media_id_set)
    for media_id in list(media_id_set)[0:5]:
        print('medias still to get:', medias_to_get)
        medias_to_get = medias_to_get - 1
        # GET EVENTS FOR THIS MEDIA_ID
        
        events_s3_key = f"{BRONZE_EVENTS_PREFIX}/run_id={client.run_id}/media_id={media_id}/events.json"
        events = client.list_events_for_media(media_id, start_date = start_date, end_date = end_date)
        # WRITE EVENTS TO S3
        if len(events) > 0:
            # write_json(BUCKET_NAME, events_s3_key, events)
            # print(f"Wrote {len(events)} events to S3.")
            visitor_id_set.update([e['visitor_key'] for e in events])
            
            event_dates = [datetime.fromisoformat(x['received_at'].replace("Z", "+00:00")) for x in events]
            min_event_date = datetime.date(min(event_dates))
            max_event_date = datetime.date(max(event_dates))
            if earliest_event_date is None or min_event_date < earliest_event_date: earliest_event_date = min_event_date
            if most_recent_event_date is None or max_event_date > most_recent_event_date: most_recent_event_date = max_event_date
            event_record_count += len(events)
        # BUILD MANIFEST FOR THIS MEDIA_ID
        event_manifest = build_manifest(
            run_id = client.run_id,
            endpoint = "events",
            record_count = len(events),
            status = "success",
            s3_key = f"{BUCKET_NAME}/{events_s3_key}",
            metadata = {"media_id":media_id, 'build_type':'test'}
        )
        event_manifest_full.extend([event_manifest])
    print('earliest date found is', earliest_event_date)
    print('most recent date found is', most_recent_event_date)
    # SAVE EVENT MANIFEST
    event_manifest_s3_key = f"manifests/events/{client.run_id}.json"
    # write_json(BUCKET_NAME, event_manifest_s3_key, event_manifest_full)


    # SAVE EVENTS STATE FILE
    state_payload = {
        "silver_events_path": SILVER_EVENTS_PATH,
        "earliest_event_date": datetime.strftime(earliest_event_date, '%Y-%m-%d'),
        "most_recent_event_date": datetime.strftime(most_recent_event_date, '%Y-%m-%d'),
        "event_record_count": event_record_count,
        "updated_at_utc": datetime.now(timezone.utc).isoformat()
    }
    # write_json(BUCKET_NAME, STATE_KEY, state_payload)

    # VISITOR LOGIC
    print(f'Found a total of {len(visitor_id_set)} visitors')
    
    # IF SILVER VISITORS EXISTS, GET VISITOR IDs FROM IT AND REMOVE FROM VISITOR_ID_SET
    visitors_silver_s3_key = 'silver/visitors'
    if object_exists(BUCKET_NAME, visitors_silver_s3_key):
        visitors_df = pd.read_parquet(f's3://{BUCKET_NAME}/{visitors_silver_s3_key}')
        seen_visitors = set(visitors_df['visitor_id'])
        visitor_id_set.difference(seen_visitors)
    
    # GET VISITORS
    visitors = client.get_visitors(BUCKET_NAME, visitor_id_set)

    # SAVE VISITORS
    visitors_s3_key = f'{BRONZE_VISITORS_PREFIX}/run_id={client.run_id}/visitors.json'
    # media_s3_key = f"{BRONZE_MEDIA_PREFIX}/run_id={client.run_id}/media.json"
    write_json(BUCKET_NAME, visitors_s3_key, visitors)

    # BUILD VISITORS MANIFEST
    visitors_manifest_s3_key = f"manifests/visitors/{client.run_id}.json"
    visitors_manifest = build_manifest(
        run_id = client.run_id,
        endpoint = "visitors",
        record_count = len(visitors),
        status = 'success',
        s3_key = visitors_s3_key,
        metadata = {'build_type':'test'}
    )
    write_json(BUCKET_NAME, visitors_manifest_s3_key, visitors_manifest)


if __name__ == "__main__":
    main()