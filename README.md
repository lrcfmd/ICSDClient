# ICSDClient
A python interface for accessing the ICSD API Client with the requests library. Please visit the [Fitz-Karlsruhe website](https://icsd.fiz-karlsruhe.de/index.xhtml) for further details on accessing the API. 

Note that the ICSD internal database ID for each cif file is not the same as the ICSD collection code. A search must first be performed for a collection code (or list of collection codes) and then retrieved using the resultant IDs.

## Setup

Clone the repository to a local directory and install the dependencies (recommendation is inside a python virtual environment)

```bash
git clone https://github.com/lrcfmd/ICSDClient/
cd ICSDClient
pip install requirements.txt
```

## Basic Usage 

First instantiate a client object with the username and password provided by Fitz-Karlsruhe

```python
client = ICSDClient("YOUR_USERNAME", "YOUR_PASSWORD")
```

Once this has authenticated successfully you can use this client to poll the ICSD and retrieve cif files. 

```python
cif_file = client.fetch_cif(1)
cif_files = client.fetch_cifs([1, 2, 3])
```

To download the latest version of all cifs in the ICSD, the `fetch_all_cifs()` method can be used. Please ensure that your API access will support this many downloads as this may exceed your limit. The authors take no responsibility for incorrect applications of this tool. Cifs will be saved to `./cifs/` by default, although this can be changed via the `cif_path` attribute.

```python
client.fetch_all_cifs()
client.fetch_all_cifs(cif_path='/YOUR/CIF/PATH')
```

A search of all ICSD fields can be performed, which will return the resultant ICSD IDs, with their associated compositions

```python
search = client.search("Na")
```

Once a search has been performed these can be passed to `fetch_cifs()` for bulk download.

```python
cifs = client.fetch_cifs(search)
```

These can be written to `.cif` files using the `writeout()` method. These will be saved to the `./cifs/` folder by default, but this can be changed via the `folder` parameter.

```python
client.writeout(cifs)
client.writeout(cifs, folder="/YOUR/STORAGE/PATH")
```

More advanced searches can be performed with a search dictionary. All available search fields can be viewed with `client.search_dict.keys()`. The default search type is AND however this can be changed to OR with `advanced_search(search_type="or")`. 

```python
search_dict = {"composition": "O",
               "collectioncode": "1-100"}

search = client.advanced_search(search_dict)
cifs = client.fetch_cifs(search)
```

Certain properties may be retrieved from the search without downloading a cif file through the `property_list` parameter. By default `property_list=["CollectionCode", "StructuredFormula"]`.

```python
search = client.advanced_search(search_dict, property_list=["CollectionCode", "StructuredFormula", "HMS", "CalculatedDensity"])
print(search)
```

Stored properties: CollectionCode, HMS, StructuredFormula, StructureType, Title, Authors, Reference, CellParameter, ReducedCellParameter, StandardizedCellParameter,, CellVolume, FormulaUnitsPerCell, FormulaWeight, Temperature, Pressure, RValue, SumFormula, ANXFormula, ABFormula, ChemicalName, MineralName, MineralGroup, CalculatedDensity, MeasuredDensity, PearsonSymbol, WyckoffSequence, Journal, Volume, PublicationYear, Page, Quality

Try to ensure that you log out correctly at the end of the session by calling `client.logout()`. If you are not successfully logged out you will need to wait an hour for the authorization token to expire.

A session history of all server responses can be found in `client.session_history`, make sure to save any large searches.
