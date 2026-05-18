from wistia.api_client import WistiaClient
from wistia.s3_io import write_json, read_json, object_exists
from wistia.manifest import build_manifest
# from wistia.state import read_state, write_state

from wistia.config import BUCKET_NAME, BRONZE_EVENTS_PREFIX, BRONZE_MEDIA_PREFIX, BRONZE_VISITORS_PREFIX

'''
Project Flow

Get all media
    current run_id structure is fine
    but add check that if the media file for the run_id already exists, load media.json instead

For each media_id:
    Ping event media state to see what was the most recent date for which events were saved
        Start with two years ago, if manifests/events/media_id=.../ folder doesn't exist
    For each date between most-recently-retrieved and yesterday:
        Get events page-by-page
        Save each page as events/media_id=.../page_n.json
        Update state/events/media_id=.../ with this date

Silver events will append only dates that are beyond its current range

Silver media will be a full reload from the most recent run_id obtained from the manifest

dim_media and dim_media_engagement should require no changes

'''

def main():
    import pandas as pd
    from datetime import datetime, timedelta, timezone, date
    client = WistiaClient()
    
    media_s3_key = f"{BRONZE_MEDIA_PREFIX}/run_id={client.run_id}/media.json"

    # IF MEDIA ALREADY RETRIEVED TODAY, LOAD THAT LIST
    if object_exists(BUCKET_NAME, media_s3_key):
        print('media data already retrieved today; reading from json')
        media = read_json(BUCKET_NAME, media_s3_key)
    else:
        print('media not already queried today; pinging API')
        media = client.list_media()
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

    medias_to_get = len(media)
    yesterday = datetime.date(datetime.now(tz=timezone.utc)) - timedelta(days=1)
    # START_DATE FOR API CALL IS DEFAULT TWO YEARS AGO
    for media_id in list(media_id_set)[0:15]:
        start_date = datetime.strptime(client.run_id, "%Y-%m-%d") - timedelta(days=730)

        # START_DATE WILL NEVER BE LESS THAN THE MEDIA CREATED DATE
        media_created_at = [x['created'] for x in media if x['hashed_id'] == media_id][0]
        media_created_at = datetime.strptime(media_created_at, '%Y-%m-%dT%H:%M:%S+00:00')
        print('media created at:', media_created_at)
        if media_created_at > start_date:
            start_date = media_created_at
        start_date = datetime.date(start_date)

        # DEFAULT START AT PAGE 1 OF EVENTS
        page = 1
        
        # IF STATE FILE ALREADY EXISTS, USE PAGE + 1 FOR LAST SUCCESSFUL DATE
        state_key = f"state/events/media_id={media_id}/media_event_state.json"
        if object_exists(BUCKET_NAME, state_key):
            last_state = read_json(BUCKET_NAME, state_key)
            
            try:
                start_date = datetime.strptime(last_state['last_successfully_saved_date'], '%Y-%m-%d')
                start_date = datetime.date(start_date)
            except ValueError:
                start_date = datetime.strptime(last_state['last_successfully_saved_date'], '%Y-%m-%d %H:%M:%S')
                start_date = datetime.date(start_date)
                
            page = last_state['last_successfully_saved_page'] + 1

        print(f'obtaining events for {media_id} {medias_to_get}/{len(media_id_set)} starting at {start_date}')
        while True: # loop over dates until we hit yesterday
            print(f'requesting date {start_date}')
            page = 1
            # print(f'start_date inside date loop block: {start_date}')
            # print(f'yesterday in date loop block: {yesterday}')
            if start_date > yesterday:
                start_date = yesterday
            elif start_date == yesterday:
                break
            while True: # loop over pages until empty response
                print(f'requesting page {page}')
                event_data = client.list_events_for_media(media_id, start_date, page)
                event_state_payload = {
                    'media_id':media_id,
                    'last_successfully_saved_date':str(start_date),
                    'last_successfully_saved_page':page
                }
                if len(event_data) != 0:
                    event_s3_key = f"bronze/events/media_id={media_id}/date={str(start_date)}/page_{str(page).zfill(4)}.json"
                    write_json(BUCKET_NAME, event_s3_key, event_data)
                write_json(BUCKET_NAME, state_key, event_state_payload)
                print('saved event state')
                if len(event_data) == 0:
                    break
                page += 1
            start_date = start_date + timedelta(days=1)
        medias_to_get = medias_to_get - 1
            
if __name__ == "__main__":
    main()