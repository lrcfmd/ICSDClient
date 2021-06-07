# ICSDClient
A python interface for accessing the ICSD API Client with the requests library.

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

A search of the ICSD can be performed and which will return the resultant ICSD IDs, with their associated compositions

```python
search = client.search("LiCl")
```

Once a search has been performed these can be passed to `fetch_cifs()` for bulk download.

```python
ret = client.fetch_cifs(search)
```

More advanced searches can be performed with a search dictionary. All available search fields can be viewed with `client.search_dict.keys()`. The default search type is AND however this can be changed to OR with `advanced_search(search_type="or")`. Please be aware that the maximum download limit is 20,000 cifs, after downloading these your account will be locked.

```python
search_dict = {"authors": "Rosseinsky",
               "chemicalname" : "O",
               "numberofelements": 3}

search = client.advanced_search(search_dict)
ret = client.fetch_cifs(search)
```

Try to ensure that you log out correctly at the end of the session by calling `client.logout()`. If you are not successfully logged out you will need to wait an hour for the authorization token to expire.
