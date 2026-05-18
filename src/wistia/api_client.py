class WistiaClient:
    import requests
    import boto3
    from botocore.exceptions import ClientError
    import json
    from datetime import datetime, timezone
    import pandas as pd

    s3 = boto3.client("s3")
    
    def get_api_token(self):
        import boto3
        from botocore.exceptions import ClientError
        import json
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
        from datetime import datetime, timezone
        self.token = self.get_api_token()
        self.run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.headers = {
            "X-Wistia-API-Version": "2026-03",
            "accept": "application/json",
            "authorization": f"Bearer {self.token}"
        }
        self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    
    def list_media(self) -> list[dict]:
        import requests
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


    def list_events_for_media(self, media_id:str, start_date:str='2025-05-01', page:int=1) -> list[dict]:
        import requests
        url = f"https://api.wistia.com/modern/stats/events"
        params = {
                'media_id':media_id,
                'per_page':100,
                'page':page,
                'start_date':start_date,
                'end_date':start_date
        }
        print(f"Requesting page {page} of events for media_id {media_id}")
        response = requests.get(url, headers=self.headers, params=params)
        events_data = response.json()            
        return events_data

    def get_visitors(self, BUCKET_NAME, visitor_ids:set[str]) -> list[dict]:
        import requests        
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