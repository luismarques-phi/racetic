import json
import time

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session


class EDCClient:
    base_url: str
    token_url: str
    oauth: OAuth2Session

    def __init__(self, client_id: str, client_secret: str, token_url: str, base_url: str):
        # Your client credentials
        self.base_url = base_url
        client = BackendApplicationClient(client_id=client_id)
        self.oauth = OAuth2Session(client=client)
        self.oauth.fetch_token(token_url=token_url,
                               client_id=client_id, client_secret=client_secret)

    def get_collection_tiles(self, collection_id: str):
        return self.oauth.get(self.base_url + f'/byoc/collections/{collection_id}/tiles').json()['data']

    def create_tile(self, edc_collection_id: str, path: str, sensing_time: str):
        request = {"path": path, "sensingTime": sensing_time}
        return self.oauth.post(url=self.base_url + f"/byoc/collections/{edc_collection_id}/tiles", json=request)

    def wait_for_status(self, edc_collection_id, tile_id):
        status = None
        tile = {}
        while status is None or status in {'WAITING', 'QUEUED'}:
            response = self.oauth.get(url=self.base_url + f"/byoc/collections/{edc_collection_id}/tiles/{tile_id}")
            tile = response.json()['data']
            status = tile['status']
            if status in {'WAITING', 'QUEUED'}:
                time.sleep(1)
        return status == "INGESTED", json.dumps(tile)
