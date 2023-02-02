import os
from contextlib import contextmanager
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictWriter
import zipfile
import io
from time import sleep
import requests 
from bs4 import BeautifulSoup

class ICSDHelper:
    MAX_CIFS = 500

    def __init__(self, id, pwd, verbose=False):
        self.id = id
        self.pwd = pwd
        self.query_mgr = ICSDClient(verbose)
        self.token = None
        self.verbose = verbose
        self.search_dict = self.load_search_dict()

    def connect(self):
        self.token = self.query_mgr.authorize(self.id, self.pwd)
    
    def close_connection(self):
        if self.token:
            self.query_mgr.logout(self.token)
        self.token = None

    @contextmanager
    def temp_connection(self):
        token = None
        try:
            token = self.query_mgr.authorize(self.id, self.pwd)
            yield token
        finally:
            if token:
                self.query_mgr.logout(token)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close_connection()

    def search(self, search_string):
        if self.token:
            try:
                ids = self.query_mgr.advanced_search(self.token, search_string)
            except ConnectionRefusedError as e:
                self.connect() # second attempt since stored token was rejected.
                ids = self.query_mgr.advanced_search(self.token, search_string)
        else:
            with self.temp_connection() as auth_token:
                ids = self.query_mgr.advanced_search(self.token, search_string)
            
        return ids
    
    def basic_search(self, query):
        ids = self.query_mgr.search(self.token, query)

    def build_search_string(self, search_dict, search_type='or'):
        for k, v in search_dict.items():
            if k not in self.search_dict:
                return f"Invalid search term {k} in search dict. Call client.search_dict.keys() to see available search terms"

            elif v is None:
                search_dict.pop(k)

        search_string = f" {search_type} ".join([f"{str(k)} : {str(v)}" for k, v in search_dict.items()])
        return search_string

    def fetch_cifs(self, ids, zip=False, output_file='icsd'):
        def fetch_cif_batch(ids, batch_idx):
            query = partial(
                self.query_mgr.fetch_cifs, 
                zip = zip,
                output_file = output_file)
            
            with self.temp_connection() as auth_token:
                return query(auth_token, ids, batch_idx)
        
        batched_ids = [ids[i: i + self.MAX_CIFS] for i in range(0, len(ids), self.MAX_CIFS)]
        
        if self.verbose: 
            print(f'Fetching {len(ids)} cifs in {len(batched_ids)} batches.')

        with ThreadPoolExecutor(max_workers=8) as exec:
            fut_to_ids = {exec.submit(fetch_cif_batch, batch, i + 1): batch for i, batch in enumerate(batched_ids)}
            for future in as_completed(fut_to_ids): 
                ids = fut_to_ids[future]
                try: 
                    result = future.result()
                    yield True, result
                except Exception as e:
                    # raise e
                    yield False, ids 

    def cifs_to_zip(self, ids, output_folder='./output', output_file='icsd'):
        def copy_all(from_zip, to_zip):
            for fname in from_zip.namelist():
                with from_zip.open(fname) as next_file:
                    # file name is provided as ``output_file``_CollCode{ccode}.cif
                    # extract {ccode} and fix length to 6 digits
                    ccode = fname[len(output_file) + 9: -4] 
                    ccode = f"{int(ccode):06}"
                    bio = io.BytesIO(next_file.read())
                    to_zip.writestr(f"{output_file}_{ccode}", bio.getvalue())  

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        results_file = os.path.join(output_folder, output_file+'_results.zip')
        failed_file = os.path.join(output_folder, output_file+'_failed_to_download_ids.txt')

        failed_ids = []
        with zipfile.ZipFile(results_file, mode='w') as archive:
            for success, result in self.fetch_cifs(ids, zip=True, output_file=output_file):
                if success:
                    with zipfile.ZipFile(io.BytesIO(result)) as zf1:
                        copy_all(zf1, archive)
                else:
                    failed_ids.extend(result)
                
        if failed_ids:
            with open(failed_file, 'w') as f:
                for id in failed_ids:
                    f.write(id+'\n')

    def fetch_data(self, ids, property_list=None):
        def fetch_data_batch(ids, batch_idx):
            query = partial(
                self.query_mgr.fetch_data,
                property_list=property_list)

            with self.temp_connection() as auth_token:
                return query(auth_token, ids, batch_idx)

        batched_ids = [ids[i: i + self.MAX_CIFS] for i in range(0, len(ids), self.MAX_CIFS)]
        
        if self.verbose: 
            print(f'Fetching data for {len(ids)} items in {len(batched_ids)} batches.')        
        
        with ThreadPoolExecutor(max_workers=8) as exec:
            fut_to_ids = {exec.submit(fetch_data_batch, batch, i + 1): batch for i, batch in enumerate(batched_ids)}
            for future in as_completed(fut_to_ids):
                ids = fut_to_ids[future]
                try:
                    result = future.result()
                    yield True, result # result = header, data   
                except Exception as e:
                    yield False, ids

    def data_to_csv(self, ids, output_folder='./output', output_file='icsd_data', columns=[]):
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        with open(os.path.join(output_folder, output_file+'.csv'), "w", newline='') as f:
            first = True
            failed_ids = []
            for success, result in self.fetch_data(ids, columns):
                if success:
                    csv_header, csv_data = result[0], result[1]
                    if first:
                        writer = DictWriter(f, fieldnames=csv_header)
                        writer.writeheader()
                        first = False
                    for data in csv_data:
                        line = dict(zip(csv_header, data))
                        line['CollectionCode'] = str(line['CollectionCode']).zfill(6)
                        writer.writerow(line)
                else:
                    failed_ids.extend(result)                    
        
        if failed_ids:
            with open(f'{output_file}_failed_to_download_ids.txt', 'w') as f:
                for id in failed_ids:
                    f.write(id+'\n')    
    
    
    def load_search_dict(self):
        search_dict = {"AUTHORS" : None, # BIBLIOGRAPHY : Authors name for the main (first) reference Text
                "ARTICLE" : None, #  BIBLIOGRAPHY : Title of article for the main (first) reference Text
                "PUBLICATIONYEAR" : None, #  BIBLIOGRAPHY : Year of publication of an article in the reference Numerical, integer
                "PAGEFIRST" : None, #  BIBLIOGRAPHY : First page number of an article in the referenceNumerical, integer
                "JOURNAL" : None, #  BIBLIOGRAPHY : Title of journal for the reference Text
                "VOLUME" : None, #  BIBLIOGRAPHY : Volume of the journal in the reference Numerical, integer
                "ABSTRACT" : None, #  BIBLIOGRAPHY : Abstract for the main (first) reference Text
                "KEYWORDS" : None, #  BIBLIOGRAPHY : Keywords for the main (first) reference Text
                "CELLVOLUME" : None, #  CELL SEARCH : Cell volumeNumerical, floating point
                "CALCDENSITY" : None, #  CELL SEARCH : Calculated density Numerical, floating poit
                "CELLPARAMETERS" : None, #  CELL SEARCH : Cell lenght a,b,c and angles alpha, beta, gamma separated by whitespace, i.e.: a b c alpha beta gamma, * if any value Numerical, floating point
                "SEARCH" : None, #  CELLDATACELL SEARCH : Restriction of cellparameters.experimental, reduced, standardized
                "STRUCTUREDFORMULA" : None, # A CHEMISTRY SEARCH : Search for typical chemical groups Text
                "CHEMICALNAME" : None, #  CHEMISTRY SEARCH : Search for (parts of) the chemical name Text
                "MINERALNAME" : None, #  CHEMISTRY SEARCH : Search for the mineral name Text
                "MINERALGROUP" : None, #  CHEMISTRY SEARCH : Search for the mineral group Text
                "ZVALUECHEMISTRY" : None, #  SEARCH :Number of formula units per unit cell Numerical, integer
                "ANXFORMULA" : None, #  CHEMISTRY SEARCH : Search for the ANX formula Text
                "ABFORMULA" : None, #  CHEMISTRY SEARCH : Search for the AB formula Text
                "FORMULAWEIGHT" : None, #  CHEMISTRY SEARCH : Search for the formula weight Numerical, floating point
                "NUMBEROFELEMENTS" : None, #  CHEMISTRY SEARCH : Search for number of elementsinteger
                "COMPOSITION" : None, #  CHEMISTRY SEARCH : Search for the chemical composition (including stochiometric coefficients and/or oxidation numbers: EL:Co.(min):Co.(max):Ox.(min):Ox.(max)with El=element, Co=coefficient, Ox=oxidation number) Text
                "COLLECTIONCODE" : None, #  DB INFO : ICSD collection codeNumerical, integer
                "PDFNUMBER" : None, #  DB INFO : PDF number as assigned by ICDD Text
                "RELEASE" : None, #  DB INFO : Release tagNumerical, integer, special format
                "RECORDINGDATE" : None, #  DB INFO : Recording date of an ICSD entry Numerical, integer, special format
                "MODIFICATIONDATE" : None, #  DB INFO : Modification date of an ICSD entry Numerical, integer, special format
                "COMMENT" : None, #  EXPERIMENTAL SEARCH : Search for a comment Text
                "RVALUE" : None, #  EXPERIMENTAL SEARCH : R-value of the refinement (0.00 ... 1.00) Numerical, floating point
                "TEMPERATURE" : None, #  EXPERIMENTAL SEARCH : Temperature of the measurement Numerical, floating point
                "PRESSURE" : None, #  EXPERIMENTAL SEARCH : Pressure during the measurement Numerical, floating point
                "SAMPLETYPE": None, # EXPERIMENTAL SEARCH : Search for the sample type: powder, singlecrystal
                "RADIATIONTYPE": None, # EXPERIMENTAL SEARCH : Search for the radiation type: xray, electrons, neutrons, synchotron
                "STRUCTURETYPE" : None, #  STRUCTURE TYPE : Search for predefined structure types directly Select one
                "SPACEGROUPSYMBOL" : None, #  SYMMETRY : Search for the space group symbol Text
                "SPACEGROUPNUMBER" : None, #  SYMMETRY : Search for the space group number Numerical, integer
                "BRAVAISLATTICE" : None, #  SYMMETRY : Select One: Primitive, a-centered, b-centered, c-centered, Body-centered, Rhombohedral, Face-centered Select one
                "CRYSTALSYSTEM" : None, #  SYMMETRY : Crystal system Select one
                "CRYSTALCLASS" : None, #  SYMMETRY : Search for the crystal class Text
                "LAUECLASS" : None, #  SYMMETRY : Search for predefined Laueclass: -1, -3, -3m, 2/m, 4/m, 4/mmm ,6/m 6/mmm ,m-3 ,m-3m ,mmm Select one
                "WYCKOFFSEQUENCE" : None, #  SYMMETRY : Search for the Wyckoff sequence Text
                "PEARSONSYMBOL" : None, #  SYMMETRY : Search for the Pearson symbol Text
                "INVERSIONCENTER" : None, #  SYMMETRY : Should inversion center be included? TRUE or FALSE
                "POLARAXIS" : None} #  SYMMETRY : Should polar axis be included TRUE or FALSE

        return {k.lower(): v for k, v in search_dict.items()}

class ICSDClient:
    url = 'https://icsd.fiz-karlsruhe.de/ws/'    
    STATUS_OK = 200
    STATUS_NOAUTH = 401

    def __init__(self, verbose=False, windows_client=False, timeout=15):
        self.session_history = []
        self.windows_client = windows_client
        self.timeout = timeout
        self.verbose = verbose

    def authorize(self, id, pwd, verbose=True):
        data = {"loginid": id, "password": pwd}
        headers = {'accept': 'text/plain', 'Content-Type': 'application/x-www-form-urlencoded'}

        attempts = 1
        while attempts <= 5: 
            response = requests.post(self.url+'auth/login', headers=headers, data=data)
            self.session_history.append(response)

            if response.status_code == self.STATUS_OK:
                token = response.headers['ICSD-Auth-Token']
                if self.verbose:
                    print(f'Login successful. auth token: {token}.')
                return token
            else: # try again -- TODO should depend on reason for failure
                sleep(0.1)
                if self.verbose:
                    print(f'Login attempt {attempts} failed.')
                attempts += 1
        else:
            raise ConnectionRefusedError(f'Unable to log in with id {id} and password {pwd}.')

    def logout(self, auth_token, verbose=True):
        headers = {'accept': 'text/plain', 'ICSD-Auth-Token': auth_token,}

        response = requests.get(self.url+'auth/logout', headers=headers)
        if self.verbose: 
            print(f'Logout using token {auth_token}. Status: {response.status_code}, {response.content.decode("UTF-8")}.')

        self.session_history.append(response)

        return response

    def search(self, auth_token, searchTerm, content_type=None):
        '''
        Available content EXPERIMENTAL_INORGANIC, EXPERIMENTAL_METALORGANIC, THERORETICAL_STRUCTURES
        '''
        if auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        if content_type is None:
            params = (
                ('query', searchTerm),
                ('content type', "EXPERIMENTAL_INORGANIC"),
            )

        else: 
            params = (
                ('query', searchTerm),
                ('content type', content_type),
            )

        headers = {
            'accept': 'application/xml',
            'ICSD-Auth-Token': auth_token,
        }

        response = requests.get(self.url+'search/simple', 
                                headers=headers, 
                                params=params,
                                timeout=self.timeout)

        self.session_history.append({searchTerm: response})

        search_results = [x for x in str(response.content).split("idnums")[1].split(" ")[1:-2]]
        
        compositions = self.fetch_data(search_results)
        
        return list(zip(search_results, compositions))

    def advanced_search(self, auth_token, search_string):
        def format_response(response):
            return_data = BeautifulSoup(response.content, features="xml")
            try: ret = return_data.idnums.contents[0].split(" ")
            except IndexError: ret = return_data.idnums.contents
            if self.verbose:
                print(f'Search returned {len(ret)} values.')
            return ret
        
        if self.verbose:
            print(f'Performing search {search_string}.')
        
        params = (('query', search_string),('content type', "EXPERIMENTAL_INORGANIC"))
        headers = {'accept': 'application/xml', 'ICSD-Auth-Token': auth_token}

        response = requests.get(self.url+'search/expert', 
                                headers=headers, 
                                params=params,
                                timeout=self.timeout)

        self.session_history.append({search_string: response})

        # TODO add exception handling for timeouts 
        if response.status_code == self.STATUS_OK:
            return format_response(response)
        else:
            if response.status_code == self.STATUS_NOAUTH:
                raise ConnectionRefusedError('Authenication token {auth_token} refused.')
            if self.verbose:
                print(f'Search failed. Status code {response.status_code}')  

    def fetch_data(self, auth_token, ids, batch_idx=1, property_list = None):
        """
        Available properties: CollectionCode, HMS, StructuredFormula, StructureType, 
        Title, Authors, Reference, CellParameter, ReducedCellParameter, StandardizedCellParameter, 
        CellVolume, FormulaUnitsPerCell, FormulaWeight, Temperature, Pressure, RValue, 
        SumFormula, ANXFormula, ABFormula, ChemicalName, MineralName, MineralGroup, 
        CalculatedDensity, MeasuredDensity, PearsonSymbol, WyckoffSequence, Journal, 
        Volume, PublicationYear, Page, Quality
        """
        def format_response(response):
            output = response.content.decode("UTF-8")
            header, *data = output.split('\n')
            header = header.split()
            if len(data) > 0 and data[-1] == '': # output ending with \n creates an empty entry after split('\n')
                data.pop()
            data = [line.split('\t') for line in data]
            self.session_history.append({str(ids): data})
            return header, data  

        if self.verbose:
            print(f'Fetching data for {len(ids)} items (batch {batch_idx}).')        

        headers = {'accept': 'application/csv', 'ICSD-Auth-Token': auth_token}
        
        if property_list is None: property_list = []
        params = [
            ('idnum', ids),
            ('windowsclient', self.windows_client),
            ('listSelection', ['CollectionCode', 'SumFormula', 'StructuredFormula'] + property_list)]

        response = requests.get(self.url+'csv', headers=headers, params=params)
        return format_response(response)  


    def fetch_cif(self, auth_token, id):
        if auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        headers = {
            'accept': 'application/cif',
            'ICSD-Auth-Token': auth_token,
        }

        params = (
            ('celltype', 'experimental'),
            ('windowsclient', self.windows_client),
        )
        
        response = requests.get(f'{self.url}{id}', headers=headers, params=params)
        
        self.session_history.append({id: response})

        return response.content.decode("UTF-8").strip()

    def fetch_cifs(self, auth_token, ids, batch_idx = 1, zip = False, output_file='icsd'):
        if auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        if isinstance(ids[0], tuple):
            ids = [x[0] for x in ids]
        
        if self.verbose:
            print(f'Fetching {len(ids)} cifs (batch {batch_idx}).')
        
        headers = {'accept': 'application/cif', 'ICSD-Auth-Token': auth_token}

        params = [
            ('idnum', ids),
            ('celltype', 'experimental'),
            ('windowsclient', self.windows_client),
        ]

        if zip:
            params.append(('filename', output_file))
            params.append(('filetype', 'zip'))
            response = requests.get(self.url+'cif/multiple', headers=headers, params=params)
            if response.status_code == self.STATUS_OK:
                return response.content 
            else:
                raise Exception('Failed to get cifs.')
        else:
            params.append(('filetype', 'cif'))
            response = requests.get(self.url+'cif/multiple', headers=headers, params=params)
            if response.status_code == self.STATUS_OK:
                cifs = response.content.decode("UTF-8").split('#(C)')[1:]                
                return ['#(C)'+cif for cif in cifs]
            else:
                raise Exception('Failed to get cifs.')



# examples
def test(cli: ICSDHelper):
    search_string = "numberofelements: 1 and composition: Fe"
    ids = cli.search(search_string)
    cli.data_to_csv(ids)
    cli.cifs_to_zip(ids, 'test_search')

def fetch_all_cifs():
    max_coll_code = 1_000_000
    search_string = f"collectioncode=0-{max_coll_code}"
    with ICSDHelper() as cli:
        ids = cli.search(search_string)
        cli.cifs_to_zip(ids, 'test_search')

def intermetallics(cli: ICSDHelper):
    non_metals = {'H', 'D', 'T', 'He', 
        'B', 'C', 'N', 'O', 'F', 'Ne', 
        'Si', 'P', 'S', 'Cl', 'Ar', 
        # 'Ge', 
        'As', 'Se', 'Br', 'Kr',
        # 'Sb',
        'Te', 'I', 'Xe',
        # 'Po',
        'At', 'Rn',
        'Ts', 'Og'}
    
    include_nm = ' or '.join([f'composition: {el}' for el in non_metals])
    exclude_nm = 'not (' + include_nm + ')'
    search_string = 'numberofelements: >=2 ' + exclude_nm
    
    ids = cli.search(search_string)
    cli.data_to_csv(
        ids, 
        'intermetallics_data',
        columns = ['StructuredFormula', 'ChemicalName'])
    cli.cifs_to_zip(ids, 'intermetallics_search')

def minerals(cli: ICSDHelper):
    search_string = "mineralname: *"
    search_string = 'numberofelements: >=2 and ' + search_string
    
    ids = cli.search(search_string)
    cli.data_to_csv(
        ids, 
        'minerals_data2', 
        columns = ['StructuredFormula', 'ChemicalName', 'MineralName', 'MineralGroup'])
    cli.cifs_to_zip(ids, 'minerals_search2')


def main():
    with ICSDHelper("YOUR USERNAME", "YOUR PASSWORD", verbose=True) as cli:
        test(cli)   

if __name__ == "__main__":
    main()
