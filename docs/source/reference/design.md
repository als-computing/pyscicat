# Design
Pysicat contains two parts. A set of models that match the models described in the SciCat server and a client to talk to that server.

## Models
The various methods in the {doc}`generated/pyscicat.model` module serve as self-validating data transfer classes. Each class inherits from the [pydantic](https://pydantic-docs.helpmanual.io/) `BaseModel` class. This provides several features including:
- Runtime validation of types and required vs. optional fields
- Easy round tripping with python dicts and json. 

## ScicatClient
The `ScicatClient` class in {doc}`generated/pyscicat.client.ScicatClient` performs all http communications with the SciCat server.