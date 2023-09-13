#!/usr/bin/env python
# coding: utf-8

# ingestion_simulation_dataset_ess
#
# Ingest the example simulation dataset in the specified scicat instance
# This script is provided as is, and as an example in pyScicat documentation
#
#
# Create by: Max Novelli
#            max.novelli@ess.eu
#            European Spallation Source ERIC,
#            P.O. Box 176,
#            SE-221 00, Lund, Sweden
#
#


# libraries
import json
import pyscicat.client as pyScClient
import pyscicat.model as pyScModel


# scicat configuration file
# includes scicat instance URL
# scicat user and password
scicat_configuration_file = "./data/ingestion_simulation_dataset_ess_config.json"
simulation_dataset_file = "./data/ingestion_simulation_dataset_ess.json"


# loads scicat configuration
with open(scicat_configuration_file, "r") as fh:
    scicat_config = json.load(fh)


# loads simulation information from matching json file
with open(simulation_dataset_file, "r") as fh:
    dataset_information = json.load(fh)

# instantiate a pySciCat client
scClient = pyScClient.ScicatClient(
    base_url=scicat_config["scicat"]["host"],
    username=scicat_config["scicat"]["username"],
    password=scicat_config["scicat"]["password"],
)

# create an owneable object to be used with all the other models
# all the fields are retrieved directly from the simulation information
ownable = pyScModel.Ownable(**dataset_information["ownable"])


# create dataset object from the pyscicat model
# includes ownable from previous step
dataset = pyScModel.RawDataset(**dataset_information["dataset"], **ownable.dict())


# create dataset entry in scicat
# it returns the full dataset information, including the dataset pid assigned automatically by scicat
created_dataset = scClient.upload_new_dataset(dataset)


# create origdatablock object from pyscicat model
origDataBlock = pyScModel.OrigDatablock(
    size=dataset_information["orig_datablock"]["size"],
    datasetId=created_dataset["pid"],
    dataFileList=[
        pyScModel.DataFile(**file)
        for file in dataset_information["orig_datablock"]["dataFileList"]
    ],
    **ownable.dict()
)

# create origDatablock associated with dataset in SciCat
# it returns the full object including SciCat id assigned when created
created_orig_datablock = scClient.upload_dataset_origdatablock(origDataBlock)
