# ========================================================================================================#
# IMPORTS
# ========================================================================================================#

import pandas as pd
import pyodbc 
import os

from sqlalchemy import create_engine, inspect

# ========================================================================================================#
# ETL CLASS
# ========================================================================================================#
class ETL:

    def __init__( self, server , file_dir , staging_database_name , destination_database_name ):

      
        # create sql alchemy connection
        self.connection_string_sql_alchemy                = f'mssql+pyodbc://{server}/{ staging_database_name }?driver=ODBC+Driver+17+for+SQL+Server'
        self.connection_string_sql_alchemy_db_destination = f'mssql+pyodbc://{server}/{ destination_database_name }?driver=ODBC+Driver+17+for+SQL+Server'

        # connection string SQL Server
        self.connection_string_sql_server = 'Driver={SQL Server};'f'Server={server};' 'Trusted_Connection=yes;'
        
        self.server                     = server
        self.file_dir                   = file_dir
        self.staging_database_name      = staging_database_name
        self.engine                     = create_engine( self.connection_string_sql_alchemy )
        self.engine_db_destination      = create_engine( self.connection_string_sql_alchemy_db_destination )
        self.conn                       = pyodbc.connect(  self.connection_string_sql_server )
        self.destination_database_name  = destination_database_name
        self.cursor                     = self.conn.cursor()
        print("="*100)
        print( 'Connecting Database:' )

        # check connection
        if self.conn:
            print( "Successfull Connection" )
        else:
            print( "Connection Failded" )
            
        self.cursor.close()
        self.conn.close()

    
    def extract( self ):

        print("="*100)
        print( 'STEP 1 :EXTRACT' )
        

        print("="*100)
        print( 'Checking Existing Database:' )

        # temporary connection
        conn_temp = pyodbc.connect( self.connection_string_sql_server )
        cursor_temp = conn_temp.cursor()

        # check if a database already exists
        check_db = f'''
            IF NOT EXISTS ( SELECT name FROM sys.databases WHERE name = '{self.staging_database_name}' )
                
                CREATE DATABASE {self.staging_database_name}
                
        '''
        # execute check_db query
        cursor_temp.execute(check_db)
        cursor_temp.close()
        conn_temp.close()

        print( 'Database checked' )

        # for each file in folder load csv files into database
       
        print("="*100)
        print( 'Loading Data:' )
        
        for file in os.listdir( self.file_dir ):
                
            if file.endswith( '.csv' ):
                
                df = pd.read_csv( file, encoding = 'UTF-8' )
                
                df.to_sql( name = file , con = self.engine , if_exists= 'replace', index = False)
                
                print( f'{ file } has been loaded into database' )
        
        # close engine connection
        self.engine.dispose()

        return None
        

    def transform( self ):

        print("="*100)
        print( 'STEP 2 :CLEANING DATA' )
    
        # inspect database schema
        inspector = inspect( self.engine )

        # get the list of tables in the database
        tables = inspector.get_table_names()

        # create empty dict
        tables_dict = {}
        df_Lan_final = pd.DataFrame()
        
        for file in tables:
            
            if file == 'dEstruturaDRE.csv':
                # read data
                query = 'SELECT * FROM [dbo].[dEstruturaDRE.csv]'
                df_dEstruturaDre = pd.read_sql( query , self.engine )
    
                # drop na
                df_dEstruturaDre = df_dEstruturaDre.dropna( axis = 0 )
    
                # change dtypes
                df_dEstruturaDre['id']                = df_dEstruturaDre['id'].astype( 'str' )
                df_dEstruturaDre['index']             = df_dEstruturaDre['index'].astype( 'int64' )
                df_dEstruturaDre['contaGerencial']    = df_dEstruturaDre['contaGerencial'].astype( 'str' )
                df_dEstruturaDre['subtotal']          = df_dEstruturaDre['subtotal'].astype( 'int64' )
                df_dEstruturaDre['empresa']           = df_dEstruturaDre['empresa'].astype( 'str' )
    
                # rename columns 
                df_dEstruturaDre = df_dEstruturaDre.rename(columns = 
                                        {'contaGerencial' : 'ManagementAccount', 
                                          'empresa' : 'Branch', 
                                          'subtotal' : 'Subtotal', 
                                          'index' : 'Index'}  )
    
                tables_dict[file] = df_dEstruturaDre
                print( f'{file} has been cleaned. It has {df_dEstruturaDre.shape[0]} rows ' )

            elif file == 'dPlanoConta.csv':
                
                # read data
                query = 'SELECT * FROM [dbo].[dPlanoConta.csv]'
                df_dPlanoConta = pd.read_sql( query , self.engine )
    
                # drop na
                df_dPlanoConta = df_dPlanoConta.dropna(axis = 0)
    
                # change dtypes
                df_dPlanoConta['mascaraDRE_id'] = df_dPlanoConta['mascaraDRE_id'].astype( 'str' )
    
                # rename columns
                columns = {"descricaoN1"   : "DescriptionLevel1", 
                 "descricaoN2"   : "DescriptionLevel2", 
                 "detalharN2"    : "DetailLevel2?",
                 "mascaraDRE_id" : "IncomeStatementTemplate_id",
                "tipoLancamento" : "EntryType",
                "index"          : "Index"}
    
                df_dPlanoConta = df_dPlanoConta.rename( columns = columns )   
    
                tables_dict[file] = df_dPlanoConta
                print( f'{file} has been cleaned. It has {df_dPlanoConta.shape[0]} rows ' )

            elif file == 'fOrcamento.csv':
                
                # read data
                query = 'SELECT * FROM [dbo].[fOrcamento.csv]'
                df_fOrcamento = pd.read_sql( query , self.engine )
                
                # drop na
                df_fOrcamento = df_fOrcamento.dropna(axis = 0)
    
                # change dtypes
                df_fOrcamento['competencia_data'] = pd.to_datetime( df_fOrcamento['competencia_data'] ) 
                
                # formatting integers numbers as .00
                df_fOrcamento['valor'] = df_fOrcamento['valor'].apply(lambda x: f"{x}.00" if '.' not in x else x)
                
                # replace the last . for #
                df_fOrcamento['valor'] = df_fOrcamento['valor'].apply(lambda x: '#'.join( x.rpartition('.') [::2] ))
                
                # replace the last . for nothing
                df_fOrcamento['valor'] = df_fOrcamento['valor'].apply( lambda x: x.replace('.', '') )
                
                # replace # for .
                df_fOrcamento['valor'] = df_fOrcamento['valor'].apply( lambda x: x.replace('#', '.') )
                
                # convert value
                df_fOrcamento['valor'] = df_fOrcamento['valor'].astype( 'float64' )
    
                # rename columns
                columns = {"competencia_data" : "AccrualDate",
                 "planoContas_id"   : "ChartOfAccounts_id",
                 "valor"            : "Amount"}
                df_fOrcamento = df_fOrcamento.rename( columns =  columns )
    
                tables_dict[file] = df_fOrcamento
                print( f'{file} has been cleaned. It has {df_fOrcamento.shape[0]} rows ' )

            elif file == 'fPrevisao.csv':
                            # read data
                query = 'SELECT * FROM [dbo].[fPrevisao.csv]'
                df_fPrevisao = pd.read_sql( query , self.engine )
    
                # drop na
                df_fPrevisao = df_fPrevisao.dropna(axis = 0)
    
                # change dtypes
                df_fPrevisao['competencia_data'] = pd.to_datetime( df_fPrevisao['competencia_data'] ) 
                
                # formatting integers numbers as .00
                df_fPrevisao['valor'] = df_fPrevisao['valor'].apply(lambda x: f"{x}.00" if '.' not in x else x)
                
                # replace the last . for #
                df_fPrevisao['valor'] = df_fPrevisao['valor'].apply(lambda x: '#'.join( x.rpartition('.') [::2] ))
                
                # replace the last . for nothing
                df_fPrevisao['valor'] = df_fPrevisao['valor'].apply( lambda x: x.replace('.', '') )
                
                # replace # for .
                df_fPrevisao['valor'] = df_fPrevisao['valor'].apply( lambda x: x.replace('#', '.') )
                
                # convert value
                df_fPrevisao['valor'] = df_fPrevisao['valor'].astype( 'float64' )
                
                # rename columns
                columns = {
                    "competencia_data": "AccrualDate",
                     "planoContas_id"  : "ChartOfAccounts_id",
                     "valor"           : "Amount"}
                
                df_fPrevisao = df_fPrevisao.rename( columns = columns )
    
                tables_dict[file] = df_fPrevisao
                print( f'{file} has been cleaned. It has {df_fPrevisao.shape[0]} rows ' )

            elif file.startswith( 'fLan' ):
                
                # read data
                query = f'SELECT * FROM [dbo].[{file}]'
                df_fLancamento = pd.read_sql( query , self.engine )
                
                # drop na
                df_fLancamento = df_fLancamento.dropna(axis = 0)

                # change dtypes
                df_fLancamento['competencia_data'] = pd.to_datetime( df_fLancamento['competencia_data'], dayfirst = 'True' )
    
                # formatting integers numbers as .00
                df_fLancamento['valor'] = df_fLancamento['valor'].apply(lambda x: f"{x}.00" if '.' not in x else x)
                
                # replace the last . for #
                df_fLancamento['valor'] = df_fLancamento['valor'].apply(lambda x: '#'.join( x.rpartition('.') [::2] ))
                
                # replace the last . for nothing
                df_fLancamento['valor'] = df_fLancamento['valor'].apply( lambda x: x.replace('.', '') )
                
                # replace # for .
                df_fLancamento['valor'] = df_fLancamento['valor'].apply( lambda x: x.replace('#', '.') )
                
                # convert value
                df_fLancamento['valor'] = df_fLancamento['valor'].astype( 'float64' )
                
                # rename columns
                columns = {
                    "competencia_data":"AccrualDate",
                    "planoContas_id"  : "ChartOfAccounts_id",
                    "valor"           : "Amount"
                }
    
                df_fLancamento = df_fLancamento.rename( columns = columns )
                
                df_Lan_final = pd.concat( [df_Lan_final , df_fLancamento] , ignore_index=True )
    
                print( f'Processing : {file}' )
        print( f'df_Lan_final has been cleaned. It has {df_Lan_final.shape[0]} rows.' )
    
        tables_dict['fLancamentos'] = df_Lan_final

        return tables_dict

    def load( self , tables_dict ):

        print("="*100)
        print( 'STPE 3:LOAD' )

        print("="*100)
        print( 'Creating Destination Database:' )

        conn_temp = pyodbc.connect(f'Driver={{SQL Server}};Server={self.server};Trusted_Connection=yes;')
        cursor_temp = conn_temp.cursor()
 
        # check if a database already exists
        check_db = f'''
            IF NOT EXISTS ( SELECT name FROM sys.databases WHERE name = '{self.destination_database_name}' )
                BEGIN
                CREATE DATABASE {self.destination_database_name}
                END
        '''
        # execute query
        cursor_temp.execute( check_db )
        cursor_temp.close()
        conn_temp.close()
        

        print( f'{destination_database} database has been created.' )

        print("="*100)
        print( 'Data Loading:' )

        # loading data
        for table_name, df in tables_dict.items():
    
            df.to_sql( name = table_name , con = self.engine_db_destination , if_exists= 'replace', index = False)
            print( f'{table_name} loaded into {self.destination_database_name}.' )
            
        print("="*100)
        print( 'ETL STATUS: COMPLETED' )
        print("="*100)

        # close engine connection
        self.engine_db_destination.dispose()
            
        return None 

# ========================================================================================================#
# ETL 
# ========================================================================================================#

server               = 'DESKTOP-U9M4TSR' 
file_dir             = 'D:/repos/ETL' # the directory of files to be extracted and transformed.
staging_database     = 'DRE'          # the name of database to be created (if none exist) to store raw data
destination_database = 'DRE_Cleaned'  # the database where cleaned data will be stored

ETL_DRE = ETL( server , file_dir , staging_database , destination_database )

ETL_DRE.extract()

cleaned_tables = ETL_DRE.transform()

ETL_DRE.load( cleaned_tables )