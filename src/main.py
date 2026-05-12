from wistia.api_client import WistiaClient
from wistia.s3_io import write_json, read_json, object_exists
from wistia.manifest import build_manifest
# from wistia.state import read_state, write_state

from wistia.config import BUCKET_NAME, BRONZE_EVENTS_PREFIX, BRONZE_MEDIA_PREFIX, BRONZE_VISITORS_PREFIX

def main():
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

    # GET AND WRITE EVENTS WHILE SAVING UNIQUE VISITOR IDs
    visitor_id_set = set()
    event_manifest_full = []
    for media_id in list(media_id_set)[0:5]:

        # GET EVENTS FOR THIS MEDIA_ID
        events_s3_key = f"{BRONZE_EVENTS_PREFIX}/run_id={client.run_id}/media_id={media_id}/events.json"
        events = client.list_events_for_media(media_id)

        # WRITE EVENTS TO S3
        if len(events) > 0:
            write_json(BUCKET_NAME, events_s3_key, events)
            print(f"Wrote {len(events)} events to S3.")
            visitor_id_set.update([e['visitor_key'] for e in events])
        else:
            print('No events for this piece of media. Nothing written.')

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
    
    # SAVE EVENT MANIFEST
    event_manifest_s3_key = f"manifests/events/{client.run_id}.json"
    write_json(BUCKET_NAME, event_manifest_s3_key, event_manifest_full)

    # GET VISITORS
    print(f'Found a total of {len(visitor_id_set)} visitors')
    visitors = client.get_visitors(BUCKET_NAME, visitor_id_set)

    # SAVE VISITORS
    visitors_s3_key = f'bronze/visitors/run_id={client.run_id}/visitors.json'
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