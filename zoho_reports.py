import os
import sys
import time
import json
import requests
import traceback
import numpy as np
import pandas as pd
from datetime import datetime

from models import RawDatabase
from dotenv import load_dotenv

load_dotenv()


class ZohoReports:

    def __init__(self):
        """
        Initializes the instance with necessary credentials and tokens from environment variables.
        
        Attributes: 
        ----------
        Client_ID : str
            The client ID for authentication, fetched from environment variables.
            
        Client_Secret : str
            The client secret for authentication, fetched from environment variables.
            
        REFRESH_TOKEN : str
            The refresh token used to regenerate the access token, fetched from environment variables.
            
        organization_id : str
            The organization ID, fetched from environment variables.
            
        access_token : str
            The access token needed for API requests, valid for 60 minutes. Generated using the get_zoho_books_access_token method.
        """
        
        self.Client_ID = os.getenv('Client_ID')
        self.Client_Secret = os.getenv('Client_Secret')
        self.REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')

        self.organization_id = os.getenv('Organization_ID')

        # Batch id for uniquely identifying the records of the specific run.
        self.batch_id = datetime.now().strftime('%Y%m%d%H%M%S')

        # Access token vaid for 60 min/1 hr. So process will be completed in one go
        self.access_token = self.get_zoho_books_access_token()

        self.file_path = self.get_file_path()


    def get_file_path(self):
        
        folder_path = os.path.join(os.getenv("ABSOLUTE_PATH"), 'temp')
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        return os.path.join(folder_path, 'temp.csv')


    def get_zoho_books_access_token(self):
        """
        Retrieves an access token for authenticating Zoho API requests.

        This function sends a POST request to Zoho's OAuth endpoint with client credentials and a refresh token to obtain 
        a new access token. The access token is required for calling the Zoho Reports API.

        Returns:
        -------
        str
            The access token needed for Zoho API requests.
        """

        url = "https://accounts.zoho.in/oauth/v2/token"
        data = {
            "client_id": self.Client_ID,
            "client_secret": self.Client_Secret,
            "refresh_token": self.REFRESH_TOKEN,
            "grant_type": "refresh_token"
        }
        response = requests.post(url, data=data)

        token_data = response.json()  
        return token_data["access_token"]


    def get_zoho_books_report(self, report_name, params = None, url = None):
        """
        Retrieves report data from the Zoho Books API based on the specified report name and optional parameters.

        This function sends a GET request to the Zoho Books API to obtain data for a specific report. 
        If a custom URL is not provided, it constructs the URL using the report name. The organization's ID is included 
        in the request parameters. An access token is used for authorization.

        Parameters:
        ----------
        report_name : str
            The name of the report to retrieve data for.
        
        params : dict, optional
            Additional parameters for the API request. If not provided, defaults to {"organization_id": self.organization_id}.
            
        url : str, optional
            The custom URL to use for the API request. If not provided, the function constructs a URL using the report name.

        Returns:
        -------
        dict
            The JSON response containing the requested report data from Zoho Books.
        """
        if url is None:
            url = f"https://www.zohoapis.in/books/v3/reports/{report_name}/"
        
        headers = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }
        if params:
            params['organization_id'] = self.organization_id
        else:
            params = {
                "organization_id": self.organization_id
            }
        
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            # print("Success:", response.json())
            pass
        else:
            print("Failed with status code:", response.status_code)
            print(response.text)

        return response.json()


    def creditNoteDetailsReport(self):
        """
        Retrieves credit note details from the Zoho Books API, processes the data, and saves it to a database.

        This function requests credit note details from the Zoho Books API using the specified report name in a 
        paginated manner, collecting and aggregating data across pages until all available data is retrieved or 
        a defined page limit is reached. After gathering the data, the function performs renaming and filtering 
        on the columns before saving the processed data to the "credit_note_details" table in the database.

        Notes:
        ------
        - Pagination is handled by incrementing the page number if the response indicates more pages are available. 
        The function stops fetching pages when the "has_more_page" attribute is False or when it reaches a limit of 200 pages.
        - Specific columns are renamed in the DataFrame to enhance readability before database insertion.
        - Unnecessary columns, such as "currency_code," "sales_person_id," and others, are dropped from the DataFrame before insertion.
        """

        report_name = "creditnotedetails"

        data = self.get_zoho_books_report(report_name)

        # results = []
        # results.extend(data['creditnote_details'][0]['creditnotes'])

        results = []
        
        params = {
            "page": 1,
        }

        i = 0
        while True:
            data = self.get_zoho_books_report(report_name, params)
            results.extend(data['creditnote_details'][0]['creditnotes'])
            
            # If more than one page so keep gooing until last
            try:
                if data['page_context']['has_more_page']:
                    params['page'] += 1
                else:
                    break
            except:
                break

            i += 1

            # If records are more than 100k so stop. temp for stopping the infinite loop
            if i == 200:
                break


        df = pd.DataFrame(results)
        df = df.rename(columns={'date': 'credit_date', 'bcy_total': 'credit_note_amount', 'bcy_balance': 'balance_amount', 'creditnote_id': 'credit_note_id', 'creditnote_number': 'credit_note_number'})
        df = df.drop(['currency_code', 'sales_person_id', 'associated_projects', 'project_names', 'contact', 'invoice', 'branch', 'reference_number', 'txn_posting_date'], axis = 1)

        df['batch_id'] = self.batch_id

        db = RawDatabase()

        # db.insert_df_table(df, "credit_note_details", "credit_note_id")
        db.insert_df_table(df, "credit_note_details")


    def vendorCreditDetails(self):
        """
        Retrieves vendor credit details from the Zoho Books API, processes the data, and saves it to a database.

        This function requests vendor credit details from the Zoho Books API in a paginated manner, collecting and 
        aggregating data across pages until no further data is available or a defined page limit is reached. 
        After retrieving the data, it performs specific renaming and filtering on the columns and saves 
        the processed data to the "vendor_credit_details" table in a database.

        Notes:
        ------
        - The function stops fetching additional pages if the "has_more_page" attribute in the response indicates 
        there are no more pages or if it reaches a page limit (200).
        - Column names in the DataFrame are renamed for better readability before inserting into the database.
        - Unnecessary columns, such as "currency_id," "vendor," and others, are dropped prior to database insertion.
        """

        report_name = "vendorcreditdetails"

        params = {
            "page": 1,
            "usestate": "true",
            "response_option": 1
            }

        results = []
        i = 0
        while True:
            data = self.get_zoho_books_report(report_name, params)
            results.extend(data['vendor_credit_details'][0]['vendor_credits'])
            
            # If more than one page so keep gooing until last
            try:
                if data['page_context']['has_more_page']:
                    params['page'] += 1
                else:
                    break
            except:
                break

            i += 1

            # If records are more than 100k so stop. temp for stopping the infinite loop
            if i == 200:
                break

        df = pd.DataFrame(results)

        df = df.rename(columns= {"vendor_credit_number": "credit_note", "date": "vendor_credit_date", "bcy_total": "amount", "bcy_balance": "balance_amount"})
        df = df.drop(['currency_id', "vendor", "has_attachment", "branch", "txn_posting_date", "reference_number"], axis = 1)

        df['batch_id'] = self.batch_id
        db = RawDatabase()
        # db.insert_df_table(df, "vendor_credit_details", "vendor_credit_id")
        db.insert_df_table(df, "vendor_credit_details")


    def arAgingDetails(self):
        """
        Retrieves and processes Accounts Receivable (AR) Aging Details data from the Zoho Books API 
        and saves it to the database.

        This function requests AR aging details from the Zoho Books API using the report name "aragingdetails" 
        and specified parameters. The data is fetched in multiple pages, processed, and cleaned before being 
        saved to the "ar_aging_details" table in the database.

        Notes:
        ------
        - The function continues to request additional pages until all pages are retrieved or a maximum of 
        200 pages (to avoid potential infinite loops).
        - After fetching, the data is transformed into a DataFrame, certain columns are renamed, and 
        unnecessary columns are removed.
        - Missing values in the 'age' column for certain dates are filled with specified values.
        - The cleaned DataFrame is then inserted into the "ar_aging_details" table in the database using 
        the `RawDatabase` class.
        """
        
        report_name = "aragingdetails"

        params = {
            "page": 1,
            "per_page": 500,
            "sort_order": "A",
            "sort_column": "date",
            "interval_range": 15,
            "number_of_columns": 4,
            "interval_type": "days",
            "group_by": "none",
            "filter_by": "InvoiceDueDate.Today",
            "entity_list": "invoice",
            "is_new_flow": "true",
            "response_option": 1
        }

        results = []
        i = 0
        while True:
            # calling for data once
            data = self.get_zoho_books_report(report_name, params)
            results.extend(data['invoiceaging'][0]['invoiceaging'])

            # If more than one page so keep gooing until last
            if data['page_context']['has_more_page']:
                params['page'] += 1
            else:
                break
            
            i += 1

            # If records are more than 100k so stop. temp for stopping the infinite loop
            if i == 200:
                break

        df = pd.DataFrame(results)
        df = df.rename(columns={"entity": "type", 'balance': 'balance_due'})
        df = df.drop(["payment_expected_date", "contact"], axis = 1)

        # There are some empty value in age so for them replacing the value
        df.loc[df['date'] == "2020-03-31", 'age'] = 1653
        
        df['age'] = df['age'].apply(lambda x: 0 if x == '' else x)

        df = df[["entity_id", "date", "amount", "exchange_rate", "reminders_sent", "currency_code", "balance_due", "transaction_number", "customer_name", "customer_id", "type", "age", "status"]]


        df['batch_id'] = self.batch_id
        db = RawDatabase()

        # db.insert_df_table(df, "ar_aging_details", "entity_id")
        db.insert_df_table(df, "ar_aging_details")


    def apAgingDetails(self):
        """
        Retrieves, processes, and saves Accounts Payable (AP) Aging Details report data from Zoho Books.

        This function calls the `get_zoho_books_report` method to obtain AP Aging Details data from the Zoho Books API.
        It retrieves data in pages, accumulating the results until all pages are fetched or a limit is reached. 
        The data is then transformed and saved into the database.

        Retrieves data for each page and continues until there are no more pages or a maximum page limit is reached.
        Extracts and manipulates specific fields, including renaming columns and updating specific values.
        The processed data is saved to a database table named `ap_aging_details` using the `RawDatabase` class.

        Notes:
        ------
        - The function temporarily limits retrieval to 200 pages to prevent infinite loops if excessive records are encountered.
        - The column `age` is set to 1653 for records where the date is "2020-03-31".
        """

        report_name = "apagingdetails"

        params = {
            "page": 1,
            "per_page": 500,
            "sort_order": "A",
            "sort_column": "date",
            "aging_by": "billduedate",
            # "to_date": "2024-12-06",
            "interval_range": 15,
            "number_of_columns": 4,
            "interval_type": "days",
            "group_by": "none",
            "include_vendor_credit_notes": "false",
            "select_columns": '[{"field":"date","group":"report"},{"field":"transaction_number","group":"report"},{"field":"entity","group":"report"},{"field":"status","group":"report"},{"field":"vendor_name","group":"report"},{"field":"age","group":"report"},{"field":"amount","group":"report"},{"field":"balance","group":"report"},{"field":"due_date","group":"report"}]',
            "include_manual_journals": "false",
            "response_option": 1      
        }

        results = []
        i = 0
        while True:
            # calling for data once
            data = self.get_zoho_books_report(report_name, params)
            results.extend(data['billsaging']['group_list'][0]['group_list'][0]['group_list'])
            
            # If more than one page so keep gooing until last
            if data['page_context']['has_more_page']:
                params['page'] += 1
            else:
                break
            
            i += 1

            # If records are more than 100k so stop. temp for stopping the infinite loop
            if i == 200:
                break

        df = pd.DataFrame(results)
        df = df.rename(columns= {"amount": "bill_amount", "balance": "balance_due", "id": "ap_aging_id", "entity": "type"})
        df.loc[df['date'] == "2020-03-31", "age"] = 1653

        df['batch_id'] = self.batch_id
        db = RawDatabase()
        db.insert_df_table(df, "ap_aging_details", "ap_aging_id")


    def generalLedgerDetails(self):
        """
        Retrieves general ledger details from the Zoho Books API, processes the data, and saves it to a database.

        This function requests general ledger details from the Zoho Books API using the specified report name in a 
        paginated manner, collecting and aggregating data across pages until all available data is retrieved or 
        a defined page limit is reached. The function processes both individual transaction details and account group 
        summaries, formats the data, and then saves it to "general_ledger_groups" and "general_ledger_details" tables in the database.

        Notes:
        ------
        - Pagination is handled by incrementing the page number if the response indicates more pages are available. 
        The function stops fetching pages when the "has_more_page" attribute is False or when it reaches a limit of 200 pages.
        - Each transaction's details are enriched with a group ID and processed into a list of results, while account group 
        summaries are stored in a separate list.
        - Specific columns are reformatted and renamed in the DataFrames to enhance readability and match the database schema 
        before insertion.
        - Unnecessary columns, such as "branch," "project_ids," "account," and "net_amount," are dropped from the "general_ledger_details" DataFrame before insertion.
        - Dates in the "general_ledger_groups" DataFrame are formatted to remove prefixes and numeric columns are converted to 
        floats for accurate storage and computation.
        """

        report_name = "generalledgerdetails"

        params = {
            "page": 1,
            "usestate": "true",
            "response_option": 0,
            }


        results = []
        group = []
        i = 0
        while True:
            # calling for data once
            data = self.get_zoho_books_report(report_name, params)

            for j in range(len(data['account_transactions'])):

                if "account_transactions" in data['account_transactions'][j]:

                    # results.extend(data['account_transactions'][j]['account_transactions'])

                    for k in range(len(data['account_transactions'][j]['account_transactions'])):

                        temp = data['account_transactions'][j]['account_transactions'][k] 
                        temp['group_id'] = data['account_transactions'][j]['group_name']

                        results.append(temp)

                temp_d = {}

                temp_d['group_id'] = data['account_transactions'][j]['group_name']
                temp_d['opening_debit'] = data['account_transactions'][j]['opening_balance']['debit']
                temp_d['opening_credit'] = data['account_transactions'][j]['opening_balance']['credit']
                temp_d['opening_date'] = data['account_transactions'][j]['opening_balance']['date']

                temp_d['closing_debit'] = data['account_transactions'][j]['closing_balance']['debit']
                temp_d['closing_credit'] = data['account_transactions'][j]['closing_balance']['credit']
                temp_d['closing_date'] = data['account_transactions'][j]['closing_balance']['date']

                group.append(temp_d)
            
            # If more than one page so keep gooing until last
            if data['page_context']['has_more_page']:
                params['page'] += 1
            else:
                break
            
            i += 1

            # If records are more than 100k so stop. temp for stopping the infinite loop
            if i == 200:
                break

        # general_ledger_groups
        df1 = pd.DataFrame(group)

        df1['opening_date'] = df1['opening_date'].apply(lambda x: x[6:] if x.startswith('As On') else x)
        df1['closing_date'] = df1['closing_date'].apply(lambda x: x[6:] if x.startswith('As On') else x)

        df1["opening_date"] = pd.to_datetime(df1["opening_date"], format="%d-%m-%y").dt.strftime("%y/%m/%d")
        df1["closing_date"] = pd.to_datetime(df1["closing_date"], format="%d-%m-%y").dt.strftime("%y/%m/%d")

        for column in ['opening_debit', 'opening_credit', 'closing_debit', 'closing_credit']:
            df1[column] = df1[column].replace("", "0.00") 
            df1[column] = df1[column].str.replace(",", "")
            df1[column] = df1[column].astype(float)  

        df1['batch_id'] = self.batch_id

        # df1.to_csv("temp/temp.csv", index = False)
        # df1 = pd.read_csv("temp/temp.csv")
        df1.to_csv(self.file_path, index = False)
        df1 = pd.read_csv(self.file_path)

        db = RawDatabase()
        # db.insert_df_table(df1, "general_ledger_groups", "group_id")
        db.insert_df_table(df1, "general_ledger_groups")


        # general_ledger_details
        df = pd.DataFrame(results)

        df['branch_name'] = df['branch'].apply(lambda x: x['branch_name'])
        df['account_group'] = df['account'].apply(lambda x: x['account_group'])

        df['amount'] = df['net_amount'].apply(lambda x: x.split(' ')[0])
        df['currency'] = df['net_amount'].apply(lambda x: x.split(' ')[1] if len(x.split(' '))>1 else x)

        # df['batch_id'] = datetime.now().strftime('%Y%m%d%H%M%S')

        df = df.drop(['branch', 'project_ids', 'account', 'reference_transaction_id', 'reporting_tag', 'net_amount','contact_id'], axis = 1)

        df = df.rename(columns = {"account_name": "account", "entity_number": "transaction_number"})

        for column in ['amount', 'debit', 'credit']:
            df[column] = df[column].replace("", "0.00") 
            if column == 'amount':
                df[column] = df[column].str.replace(",", "")
            df[column] = df[column].astype(float)  

        df['batch_id'] = self.batch_id

        df.to_csv(self.file_path, index = False)
        df = pd.read_csv(self.file_path)
        
        db = RawDatabase()
        db.insert_df_table(df, "general_ledger_details")


    def get_reports(self):
        """
        Retrieves multiple reports from the Zoho Books API and processes each report individually.

        This function iterates over a predefined list of report names, calling specific functions to fetch and 
        process each report's data. For each report, the corresponding function is executed to retrieve data 
        and store it in the database. If an error occurs during any report retrieval or processing, an error 
        message and stack trace are printed.

        Notes:
        ------
        - Each report has a dedicated function for data retrieval and processing:
        - `creditnotedetails` calls `creditNoteDetailsReport`
        - `vendorcreditdetails` calls `vendorCreditDetails`
        - `aragingdetails` calls `arAgingDetails`
        - `apagingdetails` calls `apAgingDetails`
        - `generalledgerdetails` calls `generalLedgerDetails`
        - Any exceptions encountered during the process are caught, with error details and traceback printed for debugging.
        """
        

        reports_list = ["creditnotedetails", "vendorcreditdetails", "aragingdetails", "apagingdetails", "generalledgerdetails"]

        for report_name in reports_list:
            try:
                if report_name == "creditnotedetails":
                    self.creditNoteDetailsReport()

                elif report_name == "vendorcreditdetails":
                    self.vendorCreditDetails()

                elif report_name == "aragingdetails":
                    self.arAgingDetails()

                elif report_name == "apagingdetails":
                    self.apAgingDetails()

                elif report_name == "generalledgerdetails":
                    self.generalLedgerDetails()        

                print(f"{report_name}  -- Done")
            
            except Exception as e:
                print(f"Error while fetching {report_name}: {str(e)}")
                print(traceback.format_exc())

            
if __name__ == "__main__":
    
    start = time.time()

    zoho = ZohoReports()
    zoho.get_reports()

    end = time.time()

    print(f"Total time taken to complete the process: {int((end-start)/60)} min {int((end-start)%60)} sec" )
