import base64
from datetime import datetime as dt, timedelta as td
from boto3 import session
import json
import logging
import os
import requests


class ErrorCall(Exception):
    def __init__(self, status_code, reason, body=None):
        self.status_code = status_code
        self.reason = reason
        self.body = body


class CharlesSchwabAPIInterface:
    _BASE_URL = 'https://api.schwabapi.com/marketdata/v1'

    def __init__(
        self,
        api_key,
        secret,
        access_token=None,
        refresh_token=None,
        id_token=None,
        token_handler=None
    ):
        self._api_key = api_key
        self._secret = secret

        # Components are expected to be provided in initialization
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._id_token = id_token

        # function to save new access token
        self._token_handler = token_handler

    def _getAuthURL(self):
        return f'https://api.schwabapi.com/v1/oauth/authorize?client_id={self._api_key}&redirect_uri=https://127.0.0.1'

    def _getAuthHeaders(self):
        return {
            'Authorization': "Basic " + base64.b64encode(
                bytes(f"{self._api_key}:{self._secret}", 'utf-8')
            ).decode('utf-8'),
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    def _authenticate(self):
        response_url = input('Go to the link, login and then enter response URL here bro\n')

        code = f"{response_url[response_url.index('code=')+5:response_url.index('%40')]}@"

        headers = self._getAuthHeaders()

        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': 'https://127.0.0.1'
        }

        response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)

        if response.status_code != 200:
            raise Exception(f"Auth request failed [{response.status_code}]: {response.reason}")

        response = response.json()

        if self._token_handler is not None:
            self._token_handler(response)

        else:
            logging.warning(f"New access token fetched but handler function not provided")
            print(response)

        self._access_token = response['access_token']
        self._refresh_token = response['refresh_token']
        self._id_token = response['id_token']

        logging.info(f"Authentication successful")

    def weeklyRefresh(self):
        return self._authenticate()

    def _refreshToken(self):
        headers = self._getAuthHeaders()

        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self._refresh_token,
            'redirect_uri': 'https://127.0.0.1'
        }

        response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)

        if response.status_code != 200:
            try:
                data = response.json()

            except json.JSONDecodeError:
                data = None

            raise ErrorCall(response.status_code, response.reason, data)

        response = response.json()

        if self._token_handler is not None:
            self._token_handler(response)

        else:
            logging.warning(f"New access token fetched but handler function not provided")
            print(response)

        self._access_token = response['access_token']
        self._refresh_token = response['refresh_token']
        self._id_token = response['id_token']

        logging.info(f"Access token refreshed")

    def call(self, url, method='GET', params=None):
        if self._access_token is None:
            self._authenticate()

        if method == 'GET':
            res = requests.get(
                url=self._BASE_URL + url,
                headers={
                    'Authorization': f"Bearer {self._access_token}",
                },
                params={} if params is None else params
            )

        else:
            raise Exception(f"{method} calls not allowed")

        if res.status_code != 200:
            if res.status_code == 401:
                self._refreshToken()
                return self.call(url, method, params)

            else:
                try:
                    data = res.json()

                except json.JSONDecodeError:
                    data = None

                raise ErrorCall(res.status_code, res.reason, data)

        return res.json()

    def getQuotes(self, symbols):
        return self.call(
            '/quotes',
            params={
                'symbols': ','.join(symbols),
                'indicative': False
            }
        )

    def getChain(self, symbol, from_date=None, to_date=None, strike_count=40, contract_type='ALL', strategy='SINGLE'):
        if from_date is None:
            from_date = dt.now()

        if to_date is None:
            to_date = from_date + td(days=30)

        return self.call(
            '/chains',
            params={
                "symbol": symbol,
                "contractType": contract_type,
                "strikeCount": strike_count,
                "strategy": strategy,
                "fromDate": from_date.strftime('%Y-%m-%d'),
                "toDate": to_date.strftime('%Y-%m-%d'),
                "includeUnderlyingQuote": True
            }
        )


SECRET_NAME = os.getenv('ACCESS_TOKEN_SECRET')

SAVE_IN_FILE = False if os.getenv('SAVE_IN_FILE') is None else os.getenv('SAVE_IN_FILE').upper() == 'TRUE'

TZ_NAME = 'UTC' if os.getenv('TZ_NAME') is None else os.getenv('TZ_NAME')

AWS_PROFILE_NAME = 'default' if os.getenv('AWS_PROFILE_NAME') is None else os.getenv('AWS_PROFILE_NAME')


def getTokenFromFile():
    with open('.access_token.json') as f:
        access_token = json.loads(f.read())
        f.close()

    return access_token


def saveTokenInFile(access_token):
    with open('.access_token.json', 'w') as f:
        f.write(json.dumps(access_token))
        f.close()


def getAccessToken():
    try:
        return getTokenFromFile()

    except FileNotFoundError:
        pass

    s = session.Session(profile_name=AWS_PROFILE_NAME)

    c = s.client(service_name='secretsmanager')

    secret = c.get_secret_value(SecretId=SECRET_NAME)

    access_token = json.loads(secret['SecretString'])

    if SAVE_IN_FILE:
        saveTokenInFile(access_token)

    return access_token


def accessTokenHandler(access_token):
    s = session.Session(profile_name=AWS_PROFILE_NAME)

    c = s.client(service_name='secretsmanager')

    c.update_secret(SecretId=SECRET_NAME, SecretString=json.dumps(access_token))

    if SAVE_IN_FILE:
        saveTokenInFile(access_token)

    logging.info(f"New access token value updated")


def createInterface():
    token_obj = getAccessToken()

    return CharlesSchwabAPIInterface(
        api_key=os.getenv('API_KEY'),
        secret=os.getenv('API_SECRET'),
        access_token=token_obj['access_token'],
        refresh_token=token_obj['refresh_token'],
        id_token=token_obj['id_token'],
        token_handler=lambda x: accessTokenHandler(x)
    )
