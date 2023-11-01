from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from requests_oauthlib import OAuth2Session

class Spotifyhook(HttpHook):

    """
    Interact with Spotify APIs.

    :param method: the API method to be called
    :param spotify_conn_id: The Spotify connection id. The name or identifier for
        establishing a connection to Spotify that has the base API url
        and optional authentication credentials. Default params and headers
        can also be specified in the Extra field in json format.
    """

    def __init__(
            self,
            method: str = "POST",
            spotify_conn_id: str =  "spotify_conn_id",
            **kwargs,
    ) -> None:
        super().__init__(method,**kwargs)
        self.spotify_conn_id = spotify_conn_id
        self.base_url: str = ""

    # headers can be passed through directly or in the "extra" field in the connection

    def get_conn(self, headers: Optional[Dict[Any, Any]] = None) -> OAuth2Session:

        """
        Returns OAuth2 session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        """
        session = OAuth2Session()
        conn = self.get_connection(self.spotify_conn_id)
        self.extras = conn.extra_dejson.copy()

         # https://api.com/v2/read
        if conn.host and "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            self.base_url = schema + "://" + host

        self.log.info(f"Base url is: {self.base_url}")


        try:
            client_credentials = self.extras["client_credentials"]
            refresh_token = self.extras["refresh_token"]
            grant_type = self.extras["grant_type"]
        except KeyError as e:
            self.log.error(
                "Connection to {} requires value for extra field {}.".format(
                    conn.host, e.args[0]
                )
            )
            raise AirflowException(
                f"Invalid value for extra field {e.args[0]} in Connection object"
            )

        extra_headers = {"Authorization" : f"Basic {client_credentials}"}  # client_credentials= client_id:client_secret

        #"grant_type":"refresh_token"
        body = {"grant_type":grant_type,
                        grant_type: refresh_token}

        token = session.refresh_token(
            self.base_url,
            refresh_token=refresh_token,
            headers=extra_headers,
            data = body
        )

        session.headers["Authorization"] = "Bearer {}".format(token['access_token'])
        #session["headers"] = {"Authorization" : f"Bearer  {token['access_token']}"}


        if headers:
            session.headers.update(headers)

        self.log.info(f"final headers used: {session.headers}")
        return session

    def url_from_endpoint(self, endpoint: Optional[str]) -> str:
        """Overwrite parent `url_from_endpoint` method"""
        return endpoint
