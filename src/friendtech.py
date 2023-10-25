import httpx
import logging
from typing import Optional
from conf.apis import FRIENDTECH_BACKEND

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class FriendTech:
    def __init__(self, jwt: Optional[str] = None, backend_url: str = FRIENDTECH_BACKEND):
        self.backend_url = backend_url
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
        if jwt:
            self.headers['Authorization'] = f'Bearer {jwt}'

    async def _make_request(self, method: str, endpoint: str, need_auth: bool = False, **kwargs):
        """Network request with error handling."""
        if need_auth and 'Authorization' not in self.headers:
            logger.error("Attempt to make authorized request without JWT")
            raise ValueError("JWT is required for this request.")
        
        url = f'{self.backend_url}/{endpoint}'

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.request(method, url, headers=self.headers, **kwargs)
                response.raise_for_status()
                return response.json()

        except httpx.HTTPError as e:
            # Log the error with exception information
            logger.error(f"HTTP error occurred during {method} request to {endpoint}: {e}")
            if response.status_code in (401, 403):
                logger.warning("Unauthorized request. Check your JWT.")
            return None
        
    async def get_recently_joined_users(self):
        """Returns Response object with list of recently joined users"""
        return await self._make_request("GET", 'lists/recently-joined', need_auth=True)

    async def get_global_activity(self):
        """Returns Response object with list of all recent share trades on the platform"""
        return await self._make_request("GET", 'global-activity', need_auth=True)

    async def get_address_from_twitter_username(self, username):
        """Returns base address of the user"""
        return await self._make_request("GET", f'search/users?username={username}', need_auth=True)
    
    async def get_info_from_address(self, address):
        """Returns user info from address"""
        return await self._make_request("GET", f'users/{address}')

    async def get_info_from_user_id(self, user_id):
        """Returns info from userID of friendtech user."""
        return await self._make_request("GET", f'users/by-id/{user_id}')

    async def get_holders(self, address):
        """Returns token holder of an address"""
        return await self._make_request("GET", f'users/{address}/token/holders')
    
    async def get_holdings(self, address):
        """Returns token holdings of an address"""
        return await self._make_request("GET", f'users/{address}/token-holdings')