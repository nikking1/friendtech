import httpx
import logging
from typing import Optional, Union
from conf.apis import TWITTERSCORE_KEY

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TwitterScore:
    def __init__(self, api_key: str = TWITTERSCORE_KEY):
        self.api_key = api_key
        self.base_url = "https://twitterscore.io/api/v1"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def _make_request(self, endpoint: str, params: dict) -> Union[dict, None]:
        """Helper method to make requests to the TwitterScore API"""
        url = f"{self.base_url}/{endpoint}"
        params['api_key'] = self.api_key

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                return response.json()

        except httpx.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}; status code: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None
        
    async def get_twitter_score(self, username: str) -> Union[dict, None]:
        """Retrieves the Twitter score of the specified user"""
        return await self._make_request("get_twitter_score", {"username": username})
    
    async def get_twitter_info(self, twitter_id: str) -> Union[dict, None]:
        """Retrieves followers count and avatar link for the specified Twitter ID"""
        return await self._make_request("get_twitter_info", {"twitter_id": twitter_id})
    
    async def get_twitter_scores_diff(self, twitter_id: str) -> Union[dict, None]:
        """Retrieves how much the Twitter score changed for a week/month for the specified Twitter ID"""
        return await self._make_request("get_twitter_scores_diff", {"twitter_id": twitter_id})
    
    async def get_twitter_top_followers(self, twitter_id: str) -> Union[dict, None]:
        """Retrieves the top 5 followers by Twitter score for the specified Twitter ID"""
        return await self._make_request("get_twitter_top_followers", {"twitter_id": twitter_id})
    
    async def get_followers_count_history(self, twitter_id: Optional[str] = None, username: Optional[str] = None) -> Union[dict, None]:
        """Retrieves the history of followers count for the past 30 days for the specified Twitter ID or username"""
        return await self._make_request("followers_count_history", {"twitter_id": twitter_id} if twitter_id else {"username": username})