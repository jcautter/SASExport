import os
import pandas as pd
import zipfile
import saspy

class SASExport:
    sas = None
    
    def __init__(self, cfgname):
        self.cfgname = cfgname
        
    def connect(self, cfgname=None, results='pandas', log:bool=True):
        if not cfgname:
            cfgname = self.cfgname
        
        self.sas = saspy.SASsession(
            cfgname=cfgname
            , results=results
        )
        
        if log:
            print(self.log_connection())
        
    def log_connection(self):
        return str(self.sas)
    
    def submit(self, query, log=True):
        if not self.sas:
            self.connect(log=log)
            
        self.ps = self.sas.submit(query)
        if log:
            print(self.log_query())
            
    def log_query(self):
        return str(self.ps['LOG'])
        
    def download(self, path, file, unzip=True):
        if not self.sas:
            self.connect(log=log)
            
        self.sas.download(
            '{full_path}.zip'.format(
                full_path = os.path.join(path, file)
            )
            , self.sas.workpath + '{file}.zip'.format(
                file = file
            )
        )
        
        path_file_zip = '{path}.zip'.format(
            path = os.path.join(path, file)
        )
        
        if unzip:
            with zipfile.ZipFile(path_file_zip,"r") as zip_ref:
                zip_ref.extractall(path)
