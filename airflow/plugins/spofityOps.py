from typing import Any, Dict, Optional, Sequence, Union


from datetime import datetime, timedelta
import json
import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from spotifyhook import SpotifyHook



class SpotifyToGCSOperator(BaseOperator):
    """
    Export files to Google Cloud Storage from Spotify

    :param days_ago: The relative lenght of days ago for streamed songs
    :param destination_bucket: The bucket to upload to.
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket.
        If destination_path is not provided file/files will be placed in the
        main bucket path.
        If a wildcard is supplied in the destination_path argument, this is the
        prefix that will be prepended to the final destination objects' paths.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param spotify_conn_id: The spotify connection id. The name or identifier for
        establishing a connection to Spotify
    :param method: The HTTP method to use, default = "GET"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :param mime_type: The mime-type string
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "days_ago",
        "data",
        "destination_path",
        "destination_bucket",
        "impersonation_chain",
    )
    template_fields_renderers = {"headers": "json", "data": "py"}
    ui_color = "#f4a460"

    def  __init__(
		self,
        *,
		days_ago: int,
		destination_bucket: str,
		destination_path: Optional[str] = None,
		gcp_conn_id: str = "google_cloud_default",
		spotify_conn_id: str = "spotify_conn_id",
		method: str = "GET",
		data: Any = None,
		extra_options: Optional[Dict[str, Any]] = None,
		gzip: bool = False,
		mime_type: str = "csv",
		delegate_to: Optional[str] = None,
		impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwagrs,
	) -> None:
        super().__init__(**kwagrs)
        self.days_ago=days_ago
        self.endpoint = self._set_endpoint(self.days_ago)
        self.destination_path = self._set_destination_path(destination_path)
        self.destination_bucket = self._set_bucket_name(destination_bucket)
        self.gcp_conn_id = gcp_conn_id
        self.spotify_conn_id = spotify_conn_id
        self.method = method
        self.data = data
        self.extra_options = extra_options
        self.gzip = gzip
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain


    def execute(self, context: Any):
         """Run the Operator Basically to do all the complex works"""
         gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

         spotify_hook = SpotifyHook(self.method, self.spotify_conn_id)
         self._copy_single_object(gcs_hook, spotify_hook)


    def _copy_single_object(self, gcs_hook: GCSHook, spotify_hook: SpotifyHook) -> None:
        """Helper function to copy single files from spotify to GCS """
        self.log.info(
            "Executing export of file from %s to gs://%s/%s",
            self.endpoint,
            self.destination_bucket,
            self.destination_path,
		 )

        headers= {"Content-Type": "application/json"}
        data = {"params": self.data}

        response = spotify_hook.run(
            self.endpoint, data=json.dumps(data), headers=headers
        )


		# Write response as json
        song_data = response.json()

        if response.status_code not in range(200,299):
            self.log("Error from request response")
        else:
            self.log.info("Request was successful with %s", response.status_code)

            # Write response as json
            song_data = response.json()

            # Extract only useful bit of the data.

            song_names = []
            song_ids = []
            artist_names = []
            artist_ids = []
            song_durations = []
            played_at_list = []
            timestamps = []
            #popularity=[]

            # Extracting only the relevant bits of data from the json object
            for song in song_data["items"]:
                song_names.append(song["track"]["name"])
                song_ids.append(song["track"]["id"]) #new
                artist_names.append(song["track"]["album"]["artists"][0]["name"])
                artist_ids.append(song["track"]["album"]["artists"][0]["id"]) #new
                song_durations.append(song["track"]["duration_ms"]) #new
                played_at_list.append(song["played_at"])
                timestamps.append(song["played_at"][0:10])
                #popularity.append(song_data["track"]["popularity"])


             # Prepare a dictionary in order to turn it into a pandas dataframe below
            song_dict = {
                "song_id" :song_ids,
                "song_name" : song_names,
                "artist_name": artist_names,
                "artist_id" : artist_ids,
                "song_duration":song_durations,
                "played_at" : played_at_list,
                "timestamp" : timestamps,
                #"popularity": popularity
            }

            song_df = pd.DataFrame(song_dict, columns = ["song_id","song_name","artist_name","artist_id","song_duration", "played_at", "timestamp"])#"popularity"])

            now = datetime.now()
            days_ago =  datetime.now() -  timedelta(days=self.days_ago)
            #d=pd.to_datetime(now).date()

            file_name = "spotify_songs_{ago}_to_{t}.csv".format(ago=days_ago,t=now)
            song_df.to_csv(file_name)

            # Upload data to GCS
            gcs_hook.upload(
                bucket_name=self.destination_bucket,
                object_name=self.destination_path,
                filename=file_name,
                mime_type=self.mime_type,
                gzip=self.gzip,
            )

    @staticmethod
    def _set_endpoint(days_ago: int) -> str:
        today = datetime.now() -  timedelta(days=days_ago)
        yesterday =int(today.timestamp())*1000
        endpoint ="https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday)
        return endpoint

    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")



