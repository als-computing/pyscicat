from datetime import datetime
from pathlib import Path
import os

from ..ingestor import (
    Attachment,
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Issue,
    Ownable,
    ScicatIngestor,
    Severity,
    encode_thumbnail,
    get_file_mod_time,
    get_file_size)


def main():
    now_str = datetime.isoformat(datetime.utcnow()) + "Z"
    issues = []
    ingestor = ScicatIngestor(issues)
    folder = Path('/home/dylan/data/beamlines/11012/restructured/01_PVDF-unpretreated_64765-00001')
    ownable = Ownable(    
        owner="test",
        contactEmail="dmcreynolds@lbl.gov",
        createdBy="dylan",
        updatedBy="dylan",
        updatedAt=now_str,
        createdAt=now_str,
        ownerGroup="MWET",
        accessGroups=["MWET", "ingestor"])
    dataset = create_dataset(folder, ownable)
    new_pid = ingestor.create_raw_dataset(dataset)
    
    pngs = folder.glob("*.png")
    for png in pngs:
        thumbnail = create_thumbnail(new_pid, png, ownable)
        ingestor.upload_thumbnail(thumbnail)

    data_block = create_data_block(new_pid, ownable, folder)   
    ingestor.create_datablock(data_block)

def create_data_block(dataset_id: str, ownable: Ownable, folder: Path):
    datafiles = []
    for file in folder.iterdir():
        datafile = DataFile(
            path = file.name,
            size = get_file_size(file),
            time= get_file_mod_time(file),
            type="RawDatasets"
        )
        datafiles.append(datafile)
    
    return Datablock(
        datasetId = dataset_id,
        size = get_file_size(folder),
        dataFileList = datafiles,
        **ownable.dict()
    )


def create_dataset(folder: Path, ownable: Ownable) -> Dataset:

    folder_size = get_file_size(folder)
    dataset = Dataset(
        owner="test",
        contactEmail="dmcreynolds@lbl.gov",
        creationLocation="ALS11021",
        datasetName="test2",
        type=DatasetType.raw,
        instrumentId="11012",
        proposalId="none",
        dataFormat="BCS",
        principalInvestigator="Lynne Katz",
        sourceFolder=folder.as_posix(),
        size=folder_size,
        scientificMetadata={"test": "good stuff"},
        sampleId="test",
        isPublished=False,
        description="dataset description",
        creationTime=get_file_mod_time(folder),
        **ownable.dict())
    return dataset

def create_thumbnail(dataset_id: str, file: Path, ownable: Ownable):
    return Attachment(
        datasetId = dataset_id,
        thumbnail = encode_thumbnail(file),
        caption="scattering image",
        **ownable.dict()
    )


if __name__ == "__main__":
    main()