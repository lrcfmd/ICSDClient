import os
import re
import numpy as np 
import datetime
import pandas as pd 
from contextlib import contextmanager
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictWriter

import requests 
from bs4 import BeautifulSoup

def main():
    def test(cli):
        search_string = "numberofelements: 1 and composition: Fe"
        ids = cli.search(search_string)
        print(len(ids))
        cli.data_to_csv(ids)
        # cli.fetch_cifs(ids)
        # success, data = next(gen) # unpack generator
        # print(f'successful?: {success}')
        # print(data)

    with ICSDHelper('AVV9002682', 'icsd590') as cli:
        test(cli)   

    # client = ICSDClient("YOUR_USERNAME", "YOUR_PASSWORD")

    # search_dict = {"collectioncode": "1-5000"}

    # search = client.advanced_search(search_dict, 
    #          property_list=["CollectionCode", "StructuredFormula","CalculatedDensity","MeasuredDensity","CellVolume"])
    
    # data=[]
    
    # for i,item in enumerate(search):  
    #     data.append([int(item[0]),int(item[1][0]),item[1][1],item[1][2],item[1][3],item[1][4]])
    
    
    # pd_data=pd.DataFrame(data,columns=['DB_id','Col_code','name','cal_density', 'meas_density','cellvolume'])
    
    # pd_data.to_csv('densities.csv',index=True)
            

    # # search_dict = {"collectioncode": "1-100"}

    # # search = client.advanced_search(search_dict)
    # # cifs = client.fetch_cifs(search)

    # # x = client.search("Li O")
    # # cifs = client.fetch_cifs(search)

    # # client.fetch_all_cifs()
    
    # # cif = client.fetch_cif(1)
    # # client.writeout(cif)

    # client.logout()

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
        self.query_mgr.logout(self.token)
        self.token = None

    @contextmanager
    def temp_connection(self):
        try:
            token = self.query_mgr.authorize(self.id, self.pwd)
            yield token
        finally:
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
            except ConnectionError as e:
                self.connect() # second attempt since stored token was rejected.
                ids = self.query_mgr.advanced_search(self.token, search_string)
        else:
            with self.temp_connection() as auth_token:
                ids = self.query_mgr.advanced_search(self.token, search_string)
            
        return ids

    def build_search_string(self, search_dict, search_type='or'):
        for k, v in search_dict.items():
            if k not in self.search_dict:
                return f"Invalid search term {k} in search dict. Call client.search_dict.keys() to see available search terms"

            elif v is None:
                search_dict.pop(k)

        search_string = f" {search_type} ".join([f"{str(k)} : {str(v)}" for k, v in search_dict.items()])
        return search_string

    def fetch_cifs(self, ids):
        def fetch_cif_batch(ids):            
            with self.temp_connection() as auth_token:
                return self.query_mgr.fetch_cifs(auth_token, ids)
        
        batched_ids = [ids[i: i + self.MAX_CIFS] for i in range(0, len(ids), self.MAX_CIFS)]
        
        if self.verbose: 
            print(f'Fetching {len(ids)} cifs in {len(batched_ids)} batches.')

        with ThreadPoolExecutor(max_workers=8) as exec:
            fut_to_ids = {exec.submit(fetch_cif_batch, batch): batch for i, batch in enumerate(batched_ids)}
            for future in as_completed(fut_to_ids): 
                ids = fut_to_ids[future]
                try: 
                    result = future.result()
                    yield True, result
                except Exception as e:
                    yield False, ids 

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
    STATUS_OK = 200

    def __init__(self, verbose=False, windows_client=False, timeout=15):
        self.session_history = []
        self.windows_client = windows_client
        self.timeout = timeout
        self.verbose = verbose

    def authorize(self, id, pwd, verbose=True):
        data = {"loginid": id,
                "password": pwd}

        headers = {
            'accept': 'text/plain',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        response = requests.post('https://icsd.fiz-karlsruhe.de/ws/auth/login', 
                                 headers=headers, 
                                 data=data)
        
        self.session_history.append(response)

        if response.status_code == self.STATUS_OK:
            token = response.headers['ICSD-Auth-Token']
            if verbose: print(f"Authentication succeeded. Your Auth Token for this session is {token} which will expire in one hour.")
            return token
        else:
            if verbose: print(response.content)
        
    def logout(self, auth_token, verbose=True):
        headers = {
            'accept': 'text/plain',
            'ICSD-Auth-Token': auth_token,
        }

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/auth/logout', headers=headers)
        if verbose: print(response.content)

        self.session_history.append(response)

        return response

    def writeout(self, cifs, folder="./cifs/"):
        if not os.path.exists(folder):
            os.makedirs(folder)

        if not isinstance(cifs, list):
            if cifs is None:
                print("Requires a valid cif string, this string is None. Ensure download was successful")
                return 
                
            cifs = [cifs]
        
        for cif in cifs:
            icsd_code = re.search(r"_database_code_ICSD ([0-9]+)", cif).group(1)
            filename = f"icsd_{int(icsd_code):06}.cif"

            with open(os.path.join(folder, filename), "w") as f:
                for line in cif.splitlines():
                    f.write(line + "\n")

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

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/search/simple', 
                                headers=headers, 
                                params=params,
                                timeout=self.timeout)

        self.session_history.append({searchTerm: response})

        search_results = [x for x in str(response.content).split("idnums")[1].split(" ")[1:-2]]
        
        compositions = self.fetch_data(search_results)
        
        return list(zip(search_results, compositions))

    def advanced_search(self, auth_token, search_string):
        params = (
            ('query', search_string),
            ('content type', "EXPERIMENTAL_INORGANIC"),
        )

        headers = {
            'accept': 'application/xml',
            'ICSD-Auth-Token': auth_token,
        }

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/search/expert', 
                                headers=headers, 
                                params=params,
                                timeout=self.timeout)

        # TODO add exception handling for timeouts 

        self.session_history.append({search_string: response})

        soup = BeautifulSoup(response.content, features="xml")
        search_results = soup.idnums.contents[0].split(" ")
        return search_results


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

        headers = {
            'accept': 'application/csv',
            'ICSD-Auth-Token': auth_token,
        }
        if property_list is None: property_list = []
        params = [
            ('idnum', ids),
            ('windowsclient', False),
            ('listSelection', ['CollectionCode', 'SumFormula', 'StructuredFormula'] + property_list)]

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/csv', headers=headers, params=params)
        
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
        
        response = requests.get(f'https://icsd.fiz-karlsruhe.de/ws/cif/{id}', headers=headers, params=params)
        
        self.session_history.append({id: response})

        return response.content.decode("UTF-8").strip()

    def fetch_cifs(self, auth_token, ids):
        if auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        if isinstance(ids[0], tuple):
            ids = [x[0] for x in ids]

        headers = {
            'accept': 'application/cif',
            'ICSD-Auth-Token': auth_token,
        }

        params = (
            ('idnum', ids),
            ('celltype', 'experimental'),
            ('windowsclient', self.windows_client),
            ('filetype', 'cif'),
        )

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/cif/multiple', headers=headers, params=params)
        if response.status_code == self.STATUS_OK:
            cifs = response.content.decode("UTF-8").split('#(C)')[1:]
            return ['#(C)'+cif for cif in cifs]
        else:
            raise Exception('Failed to get cifs.')    

def fetch_all_cifs(self, auth_token, cif_path="./cifs/"):
    max_coll_code = 1_000_000
    with ICSDHelper() as cli:
        search_string = f"collectioncode=0-{max_coll_code}"
        ids = cli.search(search_string)
        for success, cifs in cli.fetch_cifs(ids):
            self.writeout(cifs, cif_path)


if __name__ == "__main__":
    main()
