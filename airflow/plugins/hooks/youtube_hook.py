import httpx
from airflow.hooks.base import BaseHook

class YouTubeHook(BaseHook):
    """
    Hook for the youtube data API v3
    Uses API key authentication
    """

    BASE_URL="https://www.googleapis.com/youtube/v3"
    DEFAULT_TIMEOUT =30
    DEFAULT_MAX_RESULTS = 50


    def __init__(self,api_key:str,timeout:int=DEFAULT_TIMEOUT):
        super().__init__()
        self.api_key= api_key
        self.timeout= timeout
        self.client= httpx.Client(timeout=self.timeout)

    def get_trending_videos(
            self,
            category_id:str,
            region_code:str="US",
            max_results:int =DEFAULT_MAX_RESULTS
        
    ) -> list[dict]:
        """
        Fetching trending videos for a given category,
        Returns list of parsed video dicts.
        """
        url= f"{self.BASE_URL}/videos"

        params= {
            "part":"snippet,statistics",
            "chart":"mostPopular",
            "regionCode":region_code,
            "videoCategoryId":category_id,
            "maxResults":max_results,
            "key": self.api_key
        }

        self.log.info(f"Fetching trending videos for category {category_id}")

        response=self.client.get(url,params=params)
        response.raise_for_status()

        data=response.json()
        items= data.get("items",[])

        self.log.info(f"Got {len(items)} videos for category {category_id}")

        return [self._parse_video(item,category_id) for item in items]
    
    def _parse_video(self,item:dict,category_id:str) -> dict:
        """
        Parse raw youtube api response into clean dict
        matching out kairos_raw.videos schema
        """
        snippet= item.get("snippet",{})
        statistics= item.get("statistics",{})

        return {
            "video_id": item["id"],
            "category_id": category_id,
            "category_name":snippet.get("categoryId",""),
            "title": snippet.get("title",""),
            "channel_title": snippet.get("channelTitle",""),
            "view_count": int(statistics.get("viewCount",0)),
            "like_count": int(statistics.get("likeCount",0)),
            "comment_count": int(statistics.get("commentCount",0)),
            "published_at": snippet.get("publishedAt",""),
            "region_code":"US"
        }
    def close(self):
        self.client.close()

