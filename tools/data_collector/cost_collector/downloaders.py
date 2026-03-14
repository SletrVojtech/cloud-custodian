import os
import json
import logging
import requests
from botocore.exceptions import ClientError
from c7n_org.cli import get_session
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient


def run_aws_worker_process(account, export_name, region, output_dir):
    worker = AWS_Export_Worker(account, export_name, region, output_dir)
    return worker.run()

def run_azure_worker_process(scope_type, scope_id, output_dir, export_name=None):
    worker = Azure_Export_Worker(scope_type, scope_id, output_dir, export_name)
    return worker.run()


class AWS_Export_Worker:
    

    def __init__(self, account, export_name, region, output_dir):
        self.account = account
        self.export_name = export_name
        self.region = region
        self.output_dir = output_dir
        self.account_name = account.get('name', 'unknown')
        self.account_id = account.get('account_id')
        self.log = logging.getLogger("cost_download_worker")

    def _get_export(self):
        target_arn = None
        self.session = get_session(self.account, 'custodian', self.region)
        bcm = self.session.client('bcm-data-exports')
        # iterate through all available exports
        paginator = bcm.get_paginator('list_exports')
        for page in paginator.paginate():
            for exp in page.get('Exports', []):
                if exp.get('ExportName') == self.export_name:
                    target_arn = exp.get('ExportArn')
                    break
            if target_arn:
                break
        
        if not target_arn:
            raise ValueError(f"Export '{self.export_name}' not found.")
        # Based on https://docs.aws.amazon.com/boto3/latest/reference/services/bcm-data-exports/client/get_export.html
        full_export = bcm.get_export(ExportArn=target_arn)
        dest = full_export['Export']['DestinationConfigurations']['S3Destination']
        return dest['S3Bucket'], dest['S3Prefix']
    
    def _find_newest_Manifest(self):
        # Based on https://docs.aws.amazon.com/boto3/latest/reference/services/s3/paginator/ListObjectsV2.html
        manifest_prefix = f"{self.prefix}/{self.export_name}/metadata/"
        paginator = self.s3.get_paginator('list_objects_v2')
        latest_manifest_key, latest_time = None, None
        # iterate over the existing manifests.
        for page in paginator.paginate(Bucket=self.bucket, Prefix=manifest_prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.json') and 'Manifest.json' in obj['Key']:
                    if not latest_time or obj['LastModified'] > latest_time:
                        latest_time, latest_manifest_key = obj['LastModified'], obj['Key']

        if not latest_manifest_key:
            self.log.warning(f"[{self.account_name}] Manifest not found in s3://{self.bucket}/{manifest_prefix}")
            return None

        return latest_manifest_key
    def _download_files(self, latest_manifest_key):
        downloaded_files = []
        manifest_obj = self.s3.get_object(Bucket=self.bucket, Key=latest_manifest_key)
        manifest_data = json.loads(manifest_obj['Body'].read().decode('utf-8'))
        for s3_key in manifest_data.get('dataFiles', manifest_data.get('dataKeys', [])):
            file_name = s3_key.split('/')[-1]
            target_path = os.path.join(self.target_folder, file_name)
            if s3_key.startswith("s3://"):
                s3_key = s3_key.split('/',3)[-1]
            self.s3.download_file(self.bucket, s3_key, target_path)
            downloaded_files.append(target_path)
        return downloaded_files


    def run(self):
        self.log.info(f"[{self.account_name}] Begin download cost export: {self.export_name}")
        try:
            self.bucket, self.prefix = self._get_export()
            self.s3 = self.session.client('s3')
            latest_manifest_key = self._find_newest_Manifest()
            if not latest_manifest_key:
                return [], True
            self.target_folder = os.path.join(self.output_dir, f"aws_{self.account_name}")
            os.makedirs(self.target_folder, exist_ok=True)
            downloaded_files = self._download_files(latest_manifest_key)
            self.log.info(f"[{self.account_name}] Successfully downloaded {len(downloaded_files)} files.")
            return downloaded_files, True
        except Exception as e:
            self.log.error(f"[{self.account_name}] Error during AWS data download: {e}")
            return [], False




class Azure_Export_Worker:
    
    def __init__(self, scope_type: str, scope_id: str, output_dir: str, export_name: str):
        if scope_type == 'billing':
            self.scope = f"/providers/Microsoft.Billing/billingAccounts/{scope_id}"
        elif scope_type == 'subscription':
            self.scope = f"/subscriptions/{scope_id}"
        else:
            self.scope =  None
        self.short_id = scope_id[:8]
        self.scope_id = scope_id
        self.output_dir = output_dir
        self.export_name = export_name
        self.api_version = "2025-03-01"
        self.log = logging.getLogger("azure_cost_worker")


    def _get_token(self):
        self.credential = DefaultAzureCredential()
        token = self.credential.get_token("https://management.azure.com/.default")
        self.headers = {'Authorization': f'Bearer {token.token}'}

    def _get_export_name(self):
        # Based on https://learn.microsoft.com/en-us/rest/api/cost-management/exports/get?view=rest-cost-management-2023-11-01&tabs=HTTP
        # Loads existing Cost exports for different scopes and tries to match if name was given.
        export_url = f"https://management.azure.com{self.scope}/providers/Microsoft.CostManagement/exports?api-version={self.api_version}"
        resp = requests.get(export_url, headers=self.headers)
        resp.raise_for_status()
        exports = resp.json().get('value', [])

        if not exports:
            self.log.warning(f"[Azure-{self.short_id}] No exports found for  {self.scope}.")
            return None

        if self.export_name:
            target_export = next((e for e in exports if e['name'] == self.export_name), None)
            if not target_export:
                self.log.warning(f"[Azure-{self.short_id}] No export named '{self.export_name}' found.")
                return None
            
            return target_export['name']
        else:
            return exports[0]['name']
    
    def _get_history(self, actual_export_name):
        # Based on https://learn.microsoft.com/en-us/rest/api/cost-management/exports/get-execution-history?view=rest-cost-management-2025-03-01&tabs=HTTP
        # Tries to extract information from the newest successful cost export.
        history_url = f"https://management.azure.com{self.scope}/providers/Microsoft.CostManagement/exports/{actual_export_name}/runHistory?api-version={self.api_version}"
        history_resp = requests.get(history_url, headers=self.headers)
        history_resp.raise_for_status()
        runs = history_resp.json().get('value', [])

        latest_run = next((r for r in runs if r.get('properties', {}).get('status') == 'Completed'), None)
        
        if not latest_run:
            self.log.warning(f"[Azure-{self.short_id}] No completed runs found.")
            return None

        return latest_run['properties']
    
    def _get_manifest(self, props):
        # Extracting location of the Manifest file from latest export run.
        manifest_path = props.get('manifestFile')
        if not manifest_path:
            self.log.error(f"[Azure-{self.short_id}] RunHistory doesn't contain manifestFile).")
            return None

        delivery_dest = props.get('runSettings', {}).get('deliveryInfo', {}).get('destination', {})
        resource_id = delivery_dest.get('resourceId', '')
        container_name = delivery_dest.get('container', '')
        
        if not resource_id or not container_name:
            self.log.error(f"[Azure-{self.short_id}] missing resourceId or container in runSettings.")
            return None

        storage_account_name = resource_id.split('/')[-1]

        # Needs to be a json file
        if not manifest_path.endswith('.json'):
            manifest_path = f"{manifest_path}/manifest.json"
        self.log.info(manifest_path)
        # Assemble the URL
        self.base_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/"
        manifest_url = f"{self.base_url}{manifest_path}"
        
        self.log.info(f"[Azure-{self.short_id}] Manifest URL {manifest_url}")

        # Download the Manifest file and extract the blobs
        # Based on https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python
        blob_client = BlobClient.from_blob_url(manifest_url, credential=self.credential)
        manifest_content = json.loads(blob_client.download_blob().readall())
        return manifest_content.get('blobs', [])

    def download(self, blobs):
        downloaded_files = []
        for blob_info in blobs:
            blob_path = blob_info.get('blobName',None)
            if not blob_path:
                continue
            file_name = blob_path.split('/')[-1]
            data_url = f"{self.base_url}{blob_path}"
            target_path = os.path.join(self.output_dir, file_name)
            
            self.log.info(f"[Azure-{self.short_id}] Downloading: {file_name}")
            data_client = BlobClient.from_blob_url(data_url, credential=self.credential)
            
            with open(target_path, "wb") as f:
                f.write(data_client.download_blob().readall())
                
            downloaded_files.append(target_path)
        return downloaded_files
    
    def run(self):
        self.log.info(f"[Azure-{self.short_id}] Downloading exports for {self.scope}.")
        try:
            if not self.scope:
                return [], False
            self._get_token()
            actual_export_name = self._get_export_name()
            if not actual_export_name:
                return [], True
            props = self._get_history(actual_export_name)
            if not props:
                return [], True
            blobs = self._get_manifest(props)
            target_folder = os.path.join(self.output_dir, f"azure_{self.short_id}_{actual_export_name}")
            os.makedirs(target_folder, exist_ok=True)
            if not blobs:
                return [], True
            downloaded_files = self.download(blobs)


            self.log.info(f"[Azure-{self.short_id}] Successfully downloaded {len(downloaded_files)} files.")
            return downloaded_files, True

        except requests.exceptions.HTTPError as he:
            self.log.error(f"[Azure-{self.short_id}] API Management error: {he.response.text}")
            return [], False
        except Exception as e:
            self.log.error(f"[Azure-{self.short_id}] Error during Azure data download: {e}", exc_info=True)
            return [], False

        
