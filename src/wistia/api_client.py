class WistiaClient:
    import requests
    import boto3
    from botocore.exceptions import ClientError
    import json
    from datetime import datetime, timezone
    import pandas as pd
    
    def get_api_token(self):
        secret_name = "wistia-api-token"
        region_name = "us-east-2"
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret['API_TOKEN']

    def __init__(self):
        self.token = self.get_api_token()
        self.run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.headers = {
            "X-Wistia-API-Version": "2026-03",
            "accept": "application/json",
            "authorization": f"Bearer {self.token}"
        }
        self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    
    def list_media(self) -> list[dict]:
        url = "https://api.wistia.com/modern/medias"
        all_media = []
        next_cursor = None
        while True:
            params = {
                'per_page': 20,
                "cursor[enabled]": 1,
                "sort_by": "id",
            }
            if next_cursor:
                params['cursor[after]'] = next_cursor
            response = requests.get(url, headers=self.headers, params=params, timeout=20)
            response.raise_for_status()

            page_data = response.json()

            if not page_data:
                break

            all_media.extend(page_data)
            next_cursor = page_data[-1].get('cursor')

            if not next_cursor:
                break
        print(f"Retrieved {len(all_media)} media records")
        return all_media


    def list_events_for_media(self, media_id:str, start_date:str='2025-05-01', end_date:str='2025-05-31') -> list[dict]:
        all_events = []
        url = f"https://api.wistia.com/modern/stats/events"
        page = 1
        while True:
            params = {
                'media_id':media_id,
                'per_page':100,
                'page':page,
                'start_date':start_date,
                'end_date':end_date
            }
            print(f"Requesting page {page} of events for media_id {media_id}")
            response = requests.get(url, headers=self.headers, params=params)
            
            events_data = response.json()
            if len(events_data) == 0:
                break
            
            all_events.extend(events_data)
            page += 1
        return all_events

    def get_visitors(self, BUCKET_NAME, visitor_ids:set[str]) -> list[dict]:

        
        print(f'Acquiring information for {len(visitor_ids)} visitors')
        # GET VISITORS ONE AT A TIME        
        all_visitors = []
        for v in visitor_ids:    
            url = f"https://api.wistia.com/modern/stats/visitors/{v}"
            response = requests.get(url, headers=self.headers)
            resp = [response.json()]
            all_visitors.extend(resp)
        return all_visitors
    
if __name__ == "__main__":
    print('working')