from collections import OrderedDict
from dataclasses import dataclass, field
import enum
from typing import Dict, List, Optional, Union
import h5py
import json
from datetime import datetime
import hashlib
import urllib
import base64
import logging
import os
from pathlib import Path
import re
from typing import List


import numpy as np
from pydantic import BaseModel
import requests  # for HTTP requests

from dotenv import load_dotenv

load_dotenv('.env')

SCICAT_BASEURL=os.getenv('SCICAT_BASEURL')
SCICAT_INGEST_USER=os.getenv('SCICAT_INGEST_USER')
SCICAT_INGEST_PASSWORD=os.getenv('SCICAT_INGEST_PASSWORD')

logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class ScicatCommError(Exception):
    def __init__(self, message):
        self.message = message


class Severity(str, enum.Enum):
    warning = "warning"
    fatal = "fatal"

@dataclass  
class Issue():
    severity: Severity
    stage: str
    msg: str
    exception: Union[str, None]

    class Config:
        arbitrary_types_allowed = True

class DatasetType(str, enum.Enum):
    raw = "raw"

class Ownable(BaseModel):
    ownerGroup: str
    accessGroups: List[str]

class MongoQueryable(BaseModel):
    createdBy: Optional[str]
    updatedBy: Optional[str]
    updatedAt: Optional[str]
    createdAt: Optional[str]

class Dataset(Ownable, MongoQueryable):
    pid: Optional[str]
    owner: str
    ownerEmail: Optional[str]   
    orcidOfOwner: Optional[str]
    contactEmail: str
    creationLocation: str
    creationTime: str
    datasetName: Optional[str]
    type: DatasetType
    instrumentId: str
    proposalId: str
    dataFormat: str
    principalInvestigator: str
    sourceFolder: str
    sourceFolderHost: Optional[str]
    size: Optional[int]
    packedSize: Optional[int]
    numberOfFiles: Optional[int]
    numberOfFilesArchived: Optional[int]
    scientificMetadata: Dict
    sampleId: str
    isPublished: str
    description: Optional[str]
    validationStatus: Optional[str]
    keywords: Optional[List[str]]   
    datasetName: Optional[str]
    classification: Optional[str]
    license: Optional[str]
    version: Optional[str]
    isPublished: Optional[str]

class DataFile(MongoQueryable):
    path: str
    size: int
    time: Optional[str]
    uid: Optional[str] = None
    gid: Optional[str] = None
    perm: Optional[str] = None
    


class Datablock(Ownable ):
    id: Optional[str]
    # archiveId: str = None  listed in catamel model, but comes back invalid?
    size: int
    packedSize: Optional[int]
    chkAlg: Optional[int]
    version: str = None
    dataFileList: List[DataFile]
    datasetId: str

class Attachment(Ownable):
    id: Optional[str]
    thumbnail: str
    caption: Optional[str]
    datasetId: str


class ScicatIngestor():
    # settables
    baseurl = SCICAT_BASEURL
    # timeouts = (4, 8)  # we are hitting a transmission timeout...
    timeouts = None  # we are hitting a transmission timeout...
    username = SCICAT_INGEST_USER  # default username
    password = SCICAT_INGEST_PASSWORD     # default password
    delete_existing = False
    # You should see a nice, but abbreviated table here with the logbook contents.
    token = None  # store token here
    settables = ['baseurl', 'timeouts', 'username', 'password', 'token', "job_id"]
    pid = 0  # gets set if you search for something
    entries = None  # gets set if you search for something
    datasetType = "RawDatasets"
    datasetTypes = ["RawDatasets", "DerivedDatasets", "Proposals"]
    job_id = "0"
    test = False

    def __init__(self, issues: List[Issue], **kwargs):
        self.stage = "scicat"
        self._issues = issues
        # nothing to do
        for key, value in kwargs.items():
            assert key in self.settables, f"key {key} is not a valid input argument"
            setattr(self, key, value)
        logger.info(f"Starting ingestor talking to scicat at: {self.baseurl}")
        if self.baseurl[-1] != "/":
            self.baseurl = self.baseurl + "/"
            logger.info(f"Baseurl corrected to: {self.baseurl}")
        self._get_token()

    def _get_token(self, username=None, password=None):
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        """logs in using the provided username / password combination 
        and receives token for further communication use"""
        logger.info(f"{self.job_id} Getting new token for user {username}")

        response = requests.post(
            self.baseurl + "Users/login",
            json={"username": username, "password": password},
            timeout=self.timeouts,
            stream=False,
            verify=True,
        )
        if not response.ok:
            logger.error(f'{self.job_id} ** Error received: {response}')
            err = response.json()["error"]
            logger.error(f'{self.job_id} {err["name"]}, {err["statusCode"]}: {err["message"]}')
            self.add_error(f'error getting token {err["name"]}, {err["statusCode"]}: {err["message"]}')
            return None

        data = response.json()
        # print("Response:", data)
        token = data["id"]  # not sure if semantically correct
        logger.info(f"{self.job_id} token: {token}")
        self.token = token  # store new token
        return token

    def _send_to_scicat(self, url, dataDict=None, cmd="post"):
        """ sends a command to the SciCat API server using url and token, returns the response JSON
        Get token with the getToken method"""
        if cmd == "post":
            response = requests.post(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=True,
            )
        elif cmd == "delete":
            response = requests.delete(
                url, params={"access_token": self.token}, 
                timeout=self.timeouts, 
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "get":
            response = requests.get(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        elif cmd == "patch":
            response = requests.patch(
                url,
                params={"access_token": self.token},
                json=dataDict,
                timeout=self.timeouts,
                stream=False,
                verify=self.sslVerify,
            )
        return response



    # def ingest_scan(self, filepath, run_start,  descriptor_doc, event_sample=None, thumbnails=None):
    #     logger.info(f"{self.job_id} Scicat ingestion started for {filepath}")
    #     # get token
    #     try:
    #         self.token = self._get_token(username=self.username, password=self.password)
    #     except Exception as e:
    #         self.add_error("Could not generate token. Exiting.", e)
    #         return
    #     if not self.token:
    #         self.add_error("could not create token, exiting")
    #         return

    #     logger.info(f"{self.job_id} Ingesting file {filepath}")
    #     try:
    #         projected_start_doc = project_start_doc(run_start, "app")
    #     except Exception as e:
    #         self.add_error("error projecting start document. Exiting.", e)
    #         return
        
    #     if can_debug:
    #         logger.debug(f"{self.job_id} projected start doc: {json.dumps(projected_start_doc)}")
    #     access_controls = calculate_access_controls(self.username, projected_start_doc)
    #     logger.info(f"Access controls for  {filepath}  access_groups: {access_controls.get('accessroups')} "\
    #                 f"owner_group: {access_controls.get('owner_group')}")
    #     try:
    #         self._create_sample(
    #             projected_start_doc,
    #             access_controls.get("access_groups"), 
    #             access_controls.get("owner_group"))
    #     except Exception as e:
    #         self.add_error(f"Error creating sample for {filepath}. Continuing without sample.", e)
        
    #     try:
    #         scientific_metadata = self._extract_scientific_metadata(descriptor_doc, event_sample, run_start=run_start)
    #     except Exception as e:
    #         self.add_error(f"Error getting scientific metadata. Continuing without.", e)

    #     try:
    #         self._create_raw_dataset(
    #             projected_start_doc,
    #             scientific_metadata,
    #             access_controls.get("access_groups"), 
    #             access_controls.get("owner_group"),
    #             filepath,
    #             thumbnails)
    #     except Exception as e:
    #         self.add_error("Error creating raw data set.", e)


    def create_sample(self, projected_start_doc, access_groups, owner_group):
        sample = {
            "sampleId": projected_start_doc.get('sample_id'),
            "owner": projected_start_doc.get('pi_name'),
            "description": projected_start_doc.get('sample_name'),
            "createdAt": datetime.isoformat(datetime.utcnow()) + "Z",
            "sampleCharacteristics": {},
            "isPublished": False,
            "ownerGroup": owner_group,
            "accessGroups": access_groups,
            "createdBy": self.username,
            "updatedBy": self.username,
            "updatedAt": datetime.isoformat(datetime.utcnow()) + "Z"
        }
        sample_url = f'{self.baseurl}Samples'

        resp = self._send_to_scicat(sample_url, sample)
        if not resp.ok:  # can happen if sample id is a duplicate, but we can't tell that from the response
            err = resp.json()["error"]
            raise ScicatCommError(f"Error creating Sample {err}")

    def _get_field(self, field_name: str, projected_dict: dict, default_val):
        "some fields are required by scicat but we don't want to blow up, rather provide a default value"
        if projected_dict.get(field_name):
            return projected_dict.get(field_name)
        else:
            self.add_warning(f"missing field {field_name} defaulting to {str(default_val)}")
            return default_val

    def create_raw_dataset(self, dataset: Dataset):
        # create dataset 
        raw_dataset_url = self.baseurl + "RawDataSets/replaceOrCreate"
        resp = self._send_to_scicat(raw_dataset_url, dataset.dict(exclude_none=True))
        if not resp.ok:
            err = resp.json()["error"]
            raise ScicatCommError(f"Error creating raw dataset {err}")
        new_pid = resp.json().get('pid')
        logger.info(f"{self.job_id} new dataset created {new_pid}")
        return new_pid
        
    def create_datablock(self, datablock: Datablock):
        datasetType = "RawDatasets"
    
        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(datablock.datasetId)}/origdatablocks"
        # logger.info(f"{self.job_id} sending to {url} accessGroups: {access_groups}, ownerGroup: {owner_group}")
        # logger.info(f"datablock: {json.dumps(dataBlock)}")
        resp = self._send_to_scicat(url, datablock.dict(exclude_none=True))
        if not resp.ok:
            err = resp.json()["error"]
            raise ScicatCommError(f"Error creating datablock. {err}") 
        # logger.info(f"{self.job_id} origdatablock sent for {new_pid}")


    @staticmethod
    def _extract_scientific_metadata(descriptor, event_page, run_start=None):
        return_dict = {k.replace(":", "/"): v for k, v in descriptor['configuration']['all']['data'].items()}
        if event_page:
            return_dict['data_sample'] = event_page
        if run_start:
            return_dict['run_start_uid'] = run_start['uid']
        
        return OrderedDict(sorted(return_dict.items()))

    @staticmethod
    def _get_file_mod_time(pathobj):
        # may only work on WindowsPath objects...
        # timestamp = pathobj.lstat().st_mtime
        return str(datetime.fromtimestamp(pathobj.lstat().st_mtime))


    def upload_thumbnail(self, attachment: Attachment, datasetType="RawDatasets"):


        # dataBlock = {
        #     "caption": filename.stem,
        #     "thumbnail": encodeImageToThumbnail(filename),
        #     "datasetId": datasetId,
        #     "ownerGroup": owner_group,
        #     "accessGroups": access_groups
        # }
        url = self.baseurl + f"{datasetType}/{urllib.parse.quote_plus(attachment.datasetId)}/attachments"
        logging.debug(url)
        resp = requests.post(
                    url,
                    params={"access_token": self.token},
                    timeout=self.timeouts,
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


class NPArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def gen_ev_docs(scm: ScicatIngestor, filename: str, mapping_file: str):
    with open(mapping_file, 'r') as json_file:
        data = json.load(json_file)
    map = Mapping(**data)
    with h5py.File(filename, 'r') as h5_file:
        ingestor = MappedH5Generator(
            [],
            map,
            h5_file,
            'root',
            thumbs_root='/home/dylan/data/beamlines/als832/thumbs',
            data_groups=['als832'])
        descriptor = None
        start_doc = None
        for name, doc in ingestor.generate_docstream():
            if 'start' in name:
                start_doc = doc
                continue
            if 'descriptor' in name:
                descriptor = doc
                continue
            else:
                continue
        scm.ingest_run(Path(filename), start_doc, descriptor_doc=descriptor, thumbnail=ingestor.thumbnails[0])


def get_file_mod_time(pathobj):
    # may only work on WindowsPath objects...
    # timestamp = pathobj.lstat().st_mtime
    return str(datetime.fromtimestamp(pathobj.lstat().st_mtime))

def calculate_access_controls(username, projected_start_doc):
    # make an access grop list that includes the name of the proposal and the name of the beamline
    access_groups = []
    # set owner_group to username so that at least someone has access in case no proposal number is found
    owner_group = username
    if projected_start_doc.get('beamline'):  
        access_groups.append(projected_start_doc.get('beamline'))
        # username lets the user see the Dataset in order to ingest objects after the Dataset
        access_groups.append(username)
        # temporary mapping while beamline controls process request to match beamline name with what comes
        # from ALSHub
        if projected_start_doc.get('beamline') =="bl832":
             access_groups.append("8.3.2")

    if projected_start_doc.get('proposal') and projected_start_doc.get('proposal') != 'None':
        owner_group = projected_start_doc.get('proposal')
    
    # this is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it


    return {"owner_group": owner_group,
            "access_groups": access_groups}

def project_start_doc(start_doc, intent):
    found_projection = None
    projection = {}
    for projection in start_doc.get('projections'):
        configuration = projection.get('configuration')
        if configuration is None:
            continue
        if configuration.get('intent') == intent:
            if found_projection:
                raise Exception(f"Found more than one projection matching intent: {intent}")
            found_projection = projection
    if not found_projection:
        raise Exception(f"Could not find a projection matching intent: {intent}")
    projected_doc = {}
    for field, value in found_projection['projection'].items():
        if value['location'] == "start":
            projected_doc[field] = start_doc.get(value['field'])
    return projected_doc


def build_search_terms(projected_start):
    ''' exctract search terms from sample name to provide something pleasing to search on '''
    terms = re.split('[^a-zA-Z0-9]', projected_start.get('sample_name'))
    description = [term.lower() for term in terms if len(term) > 0]
    return ' '.join(description)

    # return "  ".join(re.sub(r'[^A-Za-z0-9 ]+', ' ', projected_start.get('sample_name')).split());

if __name__ == "__main__":
    ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    # root_logger.addHandler(ch)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)
    can_debug = logger.isEnabledFor(logging.DEBUG)
    can_info = logger.isEnabledFor(logging.INFO)
    issues = []
    scm = ScicatIngestor(password="23ljlkw", issues=issues)
    gen_ev_docs(scm, '/home/dylan/data/beamlines/als832/20210421_091523_test3.h5', './mappings/832Mapping.json')
