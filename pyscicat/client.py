from datetime import datetime
import enum

import hashlib
import urllib
import base64
import logging

import requests  # for HTTP requests


from .model import (
    Attachment,
    Datablock,
    Dataset
)

logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class ScicatCommError(Exception):
    """Represents an error encountered during communication with SciCat.
    """
    def __init__(self, message):
        self.message = message


class Severity(str, enum.Enum):
    warning = "warning"
    fatal = "fatal"


class ScicatClient():
    """Responsible for communicating with the Scicat Catamel server via http
    """

    def __init__(self, base_url: str, username: str, password: str, timeout_seconds: int = None):
        """Initialize a new instance. This method attempts to create a tokenad_a
        from the provided username and password

        Parameters
        ----------
        base_url : str
            Base url. e.g. `http://localhost:3000/api/v3`
        username : str
            username to login with
        password : str
            password to login with
        timeout_seconds : [int], optional
            timeout in seconds to wait for http connections to return, by default None
        """
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds  # we are hitting a transmission timeout...
        self._username = username  # default username
        self._password = password     # default password
        self._token = None  # store token here

        logger.info(f"Starting ingestor talking to scicat at: {self._base_url}")
        if self._base_url[-1] != "/":
            self._base_url = self._base_url + "/"
            logger.info(f"Baseurl corrected to: {self._base_url}")
        self._get_token()

    def _get_token(self, username=None, password=None):
        if username is None:
            username = self._username
        if password is None:
            password = self._password
        """logs in using the provided username / password combination
        and receives token for further communication use"""
        logger.info(f" Getting new token for user {username}")

        response = requests.post(
            self._base_url + "Users/login",
            json={"username": username, "password": password},
            timeout=self._timeout_seconds,
            stream=False,
            verify=True,
        )
        if not response.ok:
            logger.error(f' ** Error received: {response}')
            err = response.json()["error"]
            logger.error(f' {err["name"]}, {err["statusCode"]}: {err["message"]}')
            self.add_error(f'error getting token {err["name"]}, {err["statusCode"]}: {err["message"]}')
            return None

        data = response.json()
        # print("Response:", data)
        token = data["id"]  # not sure if semantically correct
        logger.info(f" token: {token}")
        self._token = token  # store new token
        return token

    def _send_to_scicat(self, url, dataDict=None, cmd="post"):
        """ sends a command to the SciCat API server using url and token, returns the response JSON
        Get token with the getToken method"""
        if cmd == "post":
            response = requests.post(
                url,
                params={"access_token": self._token},
                json=dataDict,
                timeout=self._timeout_seconds,
                stream=False,
                verify=True,
            )
        elif cmd == "delete":
            response = requests.delete(
                url, params={"access_token": self._token},
                timeout=self._timeout_seconds,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "get":
            response = requests.get(
                url,
                params={"access_token": self._token},
                json=dataDict,
                timeout=self._timeout_seconds,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "patch":
            response = requests.patch(
                url,
                params={"access_token": self._token},
                json=dataDict,
                timeout=self._timeout_seconds,
                stream=False,
                verify=self.sslVerify,
            )
        return response

    #  Future support for samples
    # def upload_sample(self, sample):
    #     sample = {
    #         "sampleId": projected_start_doc.get('sample_id'),
    #         "owner": projected_start_doc.get('pi_name'),
    #         "description": projected_start_doc.get('sample_name'),
    #         "createdAt": datetime.isoformat(datetime.utcnow()) + "Z",
    #         "sampleCharacteristics": {},
    #         "isPublished": False,
    #         "ownerGroup": owner_group,
    #         "accessGroups": access_groups,
    #         "createdBy": self._username,
    #         "updatedBy": self._username,
    #         "updatedAt": datetime.isoformat(datetime.utcnow()) + "Z"
    #     }
    #     sample_url = f'{self._base_url}Samples'

    #     resp = self._send_to_scicat(sample_url, sample)
    #     if not resp.ok:  # can happen if sample id is a duplicate, but we can't tell that from the response
    #         err = resp.json()["error"]
    #         raise ScicatCommError(f"Error creating Sample {err}")

    def upload_raw_dataset(self, dataset: Dataset) -> str:
        """Upload a raw dataset

        Parameters
        ----------
        dataset : Dataset
            Dataset to load

        Returns
        -------
        str
            pid (or unique identifier) of the newly created dataset

        Raises
        ------
        ScicatCommError
            Raises if a non-20x message is returned
        """
        raw_dataset_url = self._base_url + "RawDataSets/replaceOrCreate"
        resp = self._send_to_scicat(raw_dataset_url, dataset.dict(exclude_none=True))
        if not resp.ok:
            err = resp.json()["error"]
            raise ScicatCommError(f"Error creating raw dataset {err}")
        new_pid = resp.json().get('pid')
        logger.info(f"new dataset created {new_pid}")
        return new_pid

    def upload_datablock(self, datablock: Datablock):
        """Upload a Datablock

        Parameters
        ----------
        datablock : Datablock
            Datablock to upload

        Raises
        ------
        ScicatCommError
            Raises if a non-20x message is returned
        """
        datasetType = "RawDatasets"

        url = self._base_url + f"{datasetType}/{urllib.parse.quote_plus(datablock.datasetId)}/origdatablocks"
        resp = self._send_to_scicat(url, datablock.dict(exclude_none=True))
        if not resp.ok:
            err = resp.json()["error"]
            raise ScicatCommError(f"Error creating datablock. {err}")

    def upload_attachment(self, attachment: Attachment, datasetType: str = "RawDatasets"):
        """Upload an Attachment.  Note that datasetType can be provided to determine the type of dataset
        that this attachment is attached to. This is required for creating the url that SciCat uses.

        Parameters
        ----------
        attachment : Attachment
            Attachment to upload

        datasetType : str
            Type of dataset to upload to, default is `RawDatasets`
        Raises
        ------
        ScicatCommError
            Raises if a non-20x message is returned
        """
        url = self._base_url + f"{datasetType}/{urllib.parse.quote_plus(attachment.datasetId)}/attachments"
        logging.debug(url)
        resp = requests.post(
                    url,
                    params={"access_token": self._token},
                    timeout=self._timeout_seconds,
                    stream=False,
                    json=attachment.dict(exclude_none=True),
                    verify=True)
        if not resp.ok:
            err = resp.json()["error"]
            raise ScicatCommError(f"Error  uploading thumbnail. {err}")


def get_file_size(pathobj):
    filesize = pathobj.lstat().st_size
    return filesize


def get_checksum(pathobj):
    with open(pathobj) as file_to_check:
        # pipe contents of the file through
        return hashlib.md5(file_to_check.read()).hexdigest()


def encode_thumbnail(filename, imType='jpg'):
    logging.info(f"Creating thumbnail for dataset: {filename}")
    header = "data:image/{imType};base64,".format(imType=imType)
    with open(filename, 'rb') as f:
        data = f.read()
    dataBytes = base64.b64encode(data)
    dataStr = dataBytes.decode('UTF-8')
    return header + dataStr


def get_file_mod_time(pathobj):
    # may only work on WindowsPath objects...
    # timestamp = pathobj.lstat().st_mtime
    return str(datetime.fromtimestamp(pathobj.lstat().st_mtime))
