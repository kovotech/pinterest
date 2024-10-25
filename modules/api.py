import requests
import json
import base64
import dotenv
from modules.otherOps import update_env, convert_list_to_string

class PinterestApiError(Exception):
    pass

class PinterestAuth:

    def __init__(self,base_url:str,redirect_uri:str,app_id:str,app_secret:str) -> None:
        self.base_url=base_url
        self.redirect_uri=redirect_uri
        self.app_id=app_id
        self.app_secret=app_secret

    def get_auth_header(self):
        string_ = f"{self.app_id}:{self.app_secret}"
        string_bytes = string_.encode("ascii")
        encoded_str = base64.b64encode(string_bytes)
        auth_header = {"Authorization":f"Basic {encoded_str.decode('ascii')}"}
        return auth_header

    def get_first_token(self,auth_code:str,env_file_path:str=None):
        uri = f"{self.base_url}/oauth/token"
        response = requests.post(
                                url=uri,
                                headers=self.get_auth_header(),
                                data={
                                    "grant_type":"authorization_code",
                                    "code":auth_code,
                                    "redirect_uri":self.redirect_uri
                                }
                            )
        token_dict = json.loads(response.text)
        if env_file_path is not None:
            update_env({
                        "PINTEREST_ACCESS_TOKEN":token_dict['access_token'],
                        "PINTEREST_REFRESH_TOKEN":token_dict['refresh_token']
                        },env_file_path=env_file_path)
            return token_dict
        else:
            return token_dict

    def token_refresh(self,refresh_token:str,env_file_path:str=None):
        uri = f"{self.base_url}/oauth/token"
        response = requests.post(
                                url=uri,
                                headers=self.get_auth_header(),
                                data={
                                    "grant_type":"refresh_token",
                                    "refresh_token":refresh_token,
                                    "refresh_on":"true"
                                }
                            )
        token_dict = json.loads(response.text)
        if env_file_path is not None:
            update_env({
                        "PINTEREST_ACCESS_TOKEN":token_dict['access_token'],
                        "PINTEREST_REFRESH_TOKEN":token_dict['refresh_token']
                        },env_file_path=env_file_path)
            return token_dict
        else:
            return token_dict

class PinterestApi:
    def __init__(self,base_url:str,redirect_uri:str,app_id:str,app_secret:str,account_id:str) -> None:
        self.base_url=base_url
        self.redirect_uri=redirect_uri
        self.app_id=app_id
        self.app_secret=app_secret
        self.account_id=account_id
        self.auth=PinterestAuth(self.base_url,self.redirect_uri,self.app_id,self.app_secret)

    def get_first_token(self,auth_code:str,env_file_path:str):
        token_dict = self.auth.get_first_token(auth_code,env_file_path)
        return token_dict
    
    def token_refresh(self,refresh_token:str,env_file_path:str):
        token_dict = self.auth.token_refresh(refresh_token,env_file_path)
        return token_dict
    
    def get_campaign_list(self,page_size:int,access_token:str):
        url = f"{self.base_url}/ad_accounts/{self.account_id}/campaigns"
        bookmark = None
        last_page = False
        data = []
        while last_page is False:
            params = {"ad_account_id":self.account_id,"page_size":page_size,"bookmark":bookmark}
            headers = {"Authorization":f"Bearer {access_token}"}
            response = requests.get(
                                    url=url,
                                    params=params,
                                    headers=headers
                                )
            if response.status_code==200:
                response_data = json.loads(response.text)
                data.extend(response_data['items'])
                if response_data['bookmark'] is not None:
                    bookmark = response_data['bookmark']
                else:
                    last_page = True
            else:
                raise PinterestApiError(f"Pinterest Api Error happened:\nerrorCode:{response.status_code}\nerrorDescription:{response.text}")

        return data
    
    def get_campaign_insights(self,access_token:str,campaigns_ids:list,fields:list,start_date:str,end_date:str):
        url = f"{self.base_url}/ad_accounts/{self.account_id}/campaigns/analytics"
        params = {
                    "ad_account_id":self.account_id,
                    "granularity":"TOTAL",
                    "columns":convert_list_to_string(fields),
                    "campaign_ids":convert_list_to_string(campaigns_ids),
                    "start_date":start_date,
                    "end_date":end_date
                  }
        headers = {"Authorization":f"Bearer {access_token}"}

        response = requests.get(
                                url=url,
                                params=params,
                                headers=headers
                                )
        
        if response.status_code==200:
            return json.loads(response.text)
        else:
            raise PinterestApiError(f"Pinterest Api Error happened:\nerrorCode:{response.status_code}\nerrorDescription:{response.text}")