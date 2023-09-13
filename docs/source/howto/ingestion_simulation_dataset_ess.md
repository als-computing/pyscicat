# Ingest Simulation Dataset at ESS
In the process of designing and commissioning of the ESS, many simulation datasets have been produced in the process of finding the best design and validate them.
At ESS, we have decided to import such datsets in to our SciCat instance to facilitate search, assess quickly the comulative quality of the collected results and be able to start applying Machine Learning techniques to such data in the near future.

## Background
Data scientist and modeller at ESS have produced many simulations each one including multiple variations of the same design running parameters exploration. 
The process of ingesting all this information into SciCat will produce around a thousands new datasets.
To facilitate testing and validation of all the information at each step of the process, data curators have decided to break down the process in multiple scripts which comulative collect all the information needed to create a meaningful entry in SciCat.  
The process produces one json file containing the basic information, metadata and files associated with one datasets.
The last step is to read such file and inges it into SciCat.
The rest of this document covers all the code used to load the dataset information, create the matching models and create a new dataset and orig datablock in SciCat.

## Individual Dataset entry
Each dataset is prepared for ingestion and save in an individual json file.
The example json file is available under the example/data folder and has the following structure:

```json
{
    "id": "0275d813-be6b-444f-812f-b8311d129361", 
    "dataset": {
        "datasetName": "CAMEA CAMEA31 Hsize 4 moderator_size_y 3 PGESKSE",
        "description": "CAMEA CAMEA31 Hsize 4 moderator_size_y 3 PGESKSE", 
        "principalInvestigator": "Max Novelli", 
        "creationLocation": "DMSC", 
        "owner": "Massimiliano Novelli", 
        "ownerEmail": "max.novelli@ess.eu", 
        "contactEmail": "max.novelli@ess.eu", 
        "sourceFolder": "/mnt/data/simulation/CAMEA/CAMEA31", 
        "creationTime": "2022-03-07T15:44:59.000Z", 
        "type": "raw", 
        "techniques": [
            {
                "pid": "fe888574-5cc0-11ec-90c3-bf82943dec35", 
                "name": "Simulation"
            }
        ], 
        "size": 68386784, 
        "instrumentId": "", 
        "sampleId": "", 
        "proposalId": "",
        "scientificMetadata": {
            "sample_width": {
                "value": 0.015, 
                "unit": "m"
            }, 
            "sample_height": {
                "value": 0.015, 
                "unit": "m"
            }, 
            "divergence_requirement_horizontal": {
                "value": 0.75, 
                "unit": "deg"
            },
            "omissed" : { 
                "notes" : "Additional scientific metadata has been omitted for readability" 
            }
        }
    }, 
    "orig_datablock": {
        "size": 68386784, 
        "ownerGroup": "ess",
        "accessGroups": ["dmsc", "swap"], 
        "dataFileList": [
            {
                "path": "launch_all.sh", 
                "size": 10171, 
                "time": "2014-01-23T19:52:37.000Z"
            }, {
                "path": "suggested_reruns-fails.sh", 
                "size": 448, 
                "time": "2014-01-23T19:53:04.000Z"
            }, { 
                "notes" : "Additional files entries has been omitted for readability" 
            }
        ]
    }, 
    "ownable": {
        "ownerGroup": "ess", 
        "accessGroups": ["dmsc"]
    }
}

```
As you can see, the file has already been structure with the three main component of the dataset:
- the main dataset body with scientifica metadata
- the ownable object 
- the orig datablock containing all the files tassociated with the dataset

The three sections allows for an easier ingestion code 

## Script
The script to ingest the dataset mentioned above is available in the exampe folder with the name of `ingestion_simulation_dataset_ess.py`.
In this section, we are going to walk through the code of this script to illustrate the various functionalities.


### Overall decription
The ingestion is organized in simple sections by leveraging the dataset information which is already optimally optimized to peerform the operations required to create a full dataset in SciCat.
In order to simplify the script, it is assumed that pyscicat is installed system wide and the script is run from the folder where is saved. All the file paths are relative to the script folder.
At the beginning of the script, libraries are imported and we define paths to the relevant json files.

```python
# libraries
import json
import pyscicat.client as pyScClient
import pyscicat.model as pyScModel


# scicat configuration file
# includes scicat instance URL
# scicat user and password
scicat_configuration_file = "./data/ingestion_simulation_dataset_ess_config.json"
simulation_dataset_file = "./data/ingestion_simulation_dataset_ess_dataset.json"
```


### Loading relevant information
In the next section, the script loads the configuration needed to communicate with SciCat and the dataset information

```python
# loads scicat configuration
with open(scicat_configuration_file,"r") as fh:
    scicat_config = json.load(fh)


# loads simulation information from matching json file
with open(simulation_dataset_file,"r") as fh:
    dataset_information = json.load(fh)
```


### Authentication
Here, we instantiate the pyscicat object and perform the login.

```python
scClient = pyScClient.ScicatClient(
    base_url=scicat_config['scicat']['host'],
    username=scicat_config['scicat']['username'],
    password=scicat_config['scicat']['password']
)
```


### Create Ownable model
We, than, instantiate the ownable object, which is used in assign the correct owner and access to all the other SciCat entries that we are going to create.

```python
ownable = pyScModel.Ownable(
    **dataset_information['ownable']
)
```

This notiation is equivalent to pass in all the ownable object properties explicitly.
```python
ownable = pyScModel.Ownable(
    ownerGroup=dataset_information['ownable']['ownergroup'],
    accessGroups=dataset_information['ownable']['accessGroups']
)
```


### Create Dataset model
Next step, we need to instantiate a raw dataset object defined in pySciCat models.
Make sure to select the correct dataset: raw or derived. In our case, we are creating a raw one, which is specified in the dataset json file
```python
dataset = pyScModel.RawDataset(
    **dataset_information['dataset'],
    **ownable.dict()
)
```

As highlighted in the previous section, this notation is equivalent to assign all the model properties explicitly:
```python
dataset = pyScModel.RawDataset(
    datasetName=dataset_information['dataset']['datasetName'],
    description=dataset_information['dataset']['description'],
    creationLocation=dataset_information['dataset']['creationLocation'],
    principalInvestigator=dataset_information['dataset']['principalInvestigator'],
    owner=dataset_information['dataset']['owner'],
    ownerEmail=dataset_information['dataset']['ownerEmail'],
    ... omitted ...
    ownerGroup=dataset_information['ownable']['ownergroup'],
    accessGroups=dataset_information['ownable']['accessGroups']
)
```


### Submit Dataset to SciCat
We are now ready to make a post to SciCat and create a Dataset

```python
created_dataset = scClient.upload_new_dataset(dataset)
```

If the request is successful, the variable created_dataset should return the same information present in dataset with the additionl field named _pid_ which cotnains the official pid assigned to this dataset by SciCat


### Create OrigDatablock model
Now that we have created the dataset, we will add the list of files related to this dataset.
As we have done with the other objects, we leverage the pySciCat model to make sure that the information is properly validated.
In this snippet of code, we use explicit notation for the main object, and we use the expansion for the inner file model.

```python
origDataBlock = pyScModel.OrigDatablock(
    size=dataset_information['orig_datablock']['size'],
    datasetId=created_dataset['pid'],
    dataFileList=[
        pyScModel.DataFile(
            **file
        )
        for file
        in dataset_information['orig_datablock']['dataFileList']
    ],
    **ownable.dict()
)
```

As highlighted before, this code is equivalent to:
```python
origDataBlock = pyScModel.OrigDatablock(
    size=dataset_information['orig_datablock']['size'],
    datasetId=created_dataset['pid'],
    dataFileList=[
        pyScModel.DataFile(
            path=file['path',]
            size=file['size'],
            time=file['time']
        )
        for file
        in dataset_information['orig_datablock']['dataFileList']
    ],
    ownerGroup=dataset_information['ownable']['ownergroup'],
    accessGroups=dataset_information['ownable']['accessGroups']
)
```

### Submit OrigDatablock
With the original datablock object created, it is time to submit th erequest to SciCat.

```python
created_orig_datablock = scClient.upload_dataset_origdatablock(origDataBlock)
```

Similarly to the dataset creation function, this call will return the same information provided as argument, with the addition of the pid assigned to the entry by SciCat


## Validate the dataset
At this point, you can visit your instance of SciCat and you should see the dataset that we just created in the list of datasets. The file list can be viewed visiting the _Datafiles_ tab on the dataset details page

