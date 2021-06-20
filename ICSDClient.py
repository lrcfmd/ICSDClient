import re
import requests 
import numpy as np 

def main():
    client = ICSDClient("YOUR_USERNAME", "YOUR_PASSWORD")
    
    search = client.search("LiCl")
    ret = client.fetch_cif(718)

    cifs = client.fetch_cifs(search)

    client.logout()
    print()

class ICSDClient():
    def __init__(self, login_id=None, password=None, windows_client=True):
        self.auth_token = None 
        self.session_history = []
        self.windows_client = windows_client
        self.search_dict = self.load_search_dict()
        
        if login_id is not None:
            self.login_id = login_id
            self.password = password
            self.authorize()

    def __del__(self):
        self.logout()

    def authorize(self, verbose=True):
        data = {"loginid": self.login_id,
                "password": self.password}

        headers = {
            'accept': 'text/plain',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        response = requests.post('https://icsd.fiz-karlsruhe.de/ws/auth/login', 
                                 headers=headers, 
                                 data=data)

        if response.status_code == 200:
            self.auth_token = response.headers['ICSD-Auth-Token']
            if verbose: print(f"Authentication succeeded. Your Auth Token for this session is {self.auth_token} which will expire in one hour. Please remember to call client.logout() when you have finished.")
        else:
            if verbose: print(response.content)
        
        self.session_history.append(response)

        return response

    def logout(self, verbose=True):
        headers = {
            'accept': 'text/plain',
            'ICSD-Auth-Token': self.auth_token,
        }

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/auth/logout', headers=headers)
        if verbose: print(response.content)

        self.session_history.append(response)

        return response

    def search(self, searchTerm, content_type=None):
        '''
        Available content EXPERIMENTAL_INORGANIC, EXPERIMENTAL_METALORGANIC, THERORETICAL_STRUCTURES
        '''
        if self.auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        if content_type is None:
            params = (
                ('query', searchTerm),
            )

        else: 
            params = (
                ('query', 'LiCl'),
                ('content type', content_type),
            )

        headers = {
            'accept': 'application/xml',
            'ICSD-Auth-Token': self.auth_token,
        }

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/search/simple', headers=headers, params=params)

        self.session_history.append(response)

        search_results = [x for x in str(response.content).split("idnums")[1].split(" ")[1:-2]]
        
        compositions = self.fetch_data(search_results)
        
        return list(zip(search_results, compositions))

    def advanced_search(self, search_dict, search_type="and", property="StructuredFormula"):
        for k, v in search_dict.items():
            if k not in self.search_dict:
                return f"Invalid search term {k} in search dict. Call client.search_dict.keys() to see available search terms"

            elif v is None:
                search_dict.pop(k)

        search_string = f" {search_type} ".join([f"{str(k)} : {str(v)}" for k, v in search_dict.items()])

        
        params = (
            ('query', search_string),
        )

        headers = {
            'accept': 'application/xml',
            'ICSD-Auth-Token': self.auth_token,
        }

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/search/expert', headers=headers, params=params)

        self.session_history.append(response)

        search_results = [x for x in str(response.content).split("idnums")[1].split(" ")[1:-2]]

        properties = self.fetch_data(search_results, property=property)
        
        return list(zip(search_results, properties))

    def fetch_data(self, ids, property="StructuredFormula"):
        """
        Available properties: CollectionCode, HMS, StructuredFormula, StructureType, 
        Title, Authors, Reference, CellParameter, ReducedCellParameter, StandardizedCellParameter, 
        CellVolume, FormulaUnitsPerCell, FormulaWeight, Temperature, Pressure, RValue, 
        SumFormula, ANXFormula, ABFormula, ChemicalName, MineralName, MineralGroup, 
        CalculatedDensity, MeasuredDensity, PearsonSymbol, WyckoffSequence, Journal, 
        Volume, PublicationYear, Page, Quality
        """
        if len(ids) > 500:
            chunked_ids = np.array_split(ids, np.ceil(len(ids)/500))

            return_responses = []
            for i, chunk in enumerate(chunked_ids):
                return_responses.append(self.fetch_data(chunk))
                
                if i % 2 == 0:
                    self.logout(verbose=False)
                    self.authorize(verbose=False)

            flattened = [item for sublist in return_responses for item in sublist]

            return flattened

        headers = {
            'accept': 'application/csv',
            'ICSD-Auth-Token': self.auth_token,
        }

        params = (
            ('idnum', ids),
            ('windowsclient', self.windows_client),
            ('listSelection', property),
        )

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/csv', headers=headers, params=params)

        data = str(response.content).split("\\t\\r\\n")[1:-1]

        return data


    def fetch_cif(self, id):
        if self.auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        headers = {
            'accept': 'application/cif',
            'ICSD-Auth-Token': self.auth_token,
        }

        params = (
            ('celltype', 'experimental'),
            ('windowsclient', self.windows_client),
        )
        
        response = requests.get(f'https://icsd.fiz-karlsruhe.de/ws/cif/{id}', headers=headers, params=params)
        
        self.session_history.append(response)

        return response.content

    def fetch_cifs(self, ids):
        if self.auth_token is None:
            print("You are not authenticated, call client.authorize() first")
            return 

        if isinstance(ids[0], tuple):
            ids = [x[0] for x in ids]

        if len(ids) > 500:
            chunked_ids = np.array_split(ids, np.ceil(len(ids)/500))
            return_responses = []

            for i, chunk in enumerate(chunked_ids):
                return_responses.append(self.fetch_cifs(chunk))
                
                if i % 2 == 0:
                    self.logout(verbose=False)
                    self.authorize(verbose=False)

            flattened = [item for sublist in return_responses for item in sublist]

            return_responses = ''.join(flattened)
            cifs = re.split("\(C\) 2021 by FIZ Karlsruhe", return_responses)[1:]
            cifs = ["(C) 2021 by FIZ Karlsruhe" + x for x in cifs]

            return cifs

        headers = {
            'accept': 'application/cif',
            'ICSD-Auth-Token': self.auth_token,
        }

        params = (
            ('idnum', ids),
            ('celltype', 'experimental'),
            ('windowsclient', self.windows_client),
            ('filetype', 'cif'),
        )

        response = requests.get('https://icsd.fiz-karlsruhe.de/ws/cif/multiple', headers=headers, params=params)

        cifs = re.split("\(C\) 2021 by FIZ Karlsruhe", str(response.content))[1:]
        cifs = ["(C) 2021 by FIZ Karlsruhe" + x for x in cifs]
            
        return cifs

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
                "STRUCTUREDFORMUL" : None, # A CHEMISTRY SEARCH : Search for typical chemical groups Text
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

if __name__ == "__main__":
    main()
