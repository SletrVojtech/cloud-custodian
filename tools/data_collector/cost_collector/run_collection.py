import yaml
import sys
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter

from c7n_org.cli import init, accounts_iterator
from tools.data_collector.cost_collector.downloaders import run_aws_worker_process, run_azure_worker_process

log = logging.getLogger("cost_export_downloader")

def run_cost_downloads(output_dir="/tmp/cost_exports"):
    # Load AWS accounts
    accounts_config, _, _ = init(
        config='accounts.yml',
        use=None, debug=False, verbose=False,
        accounts=None, tags=None, policies=None
    )
    
    # Load Cost Export definitions
    with open('cost_exports.yml', 'r') as f:
        cost_config = yaml.safe_load(f)

    # Map Account ID to export name 
    aws_exports_map = {
        str(item['account_id']): item 
        for item in cost_config.get('aws', [])
    }

    worker_count = 1
    success = True
    all_downloaded_files = []

    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        futures = {}
        
        # Iterate over AWS
        for account in accounts_iterator(accounts_config):
            acc_id = str(account.get('account_id'))
            
            if acc_id in aws_exports_map:
                export_info = aws_exports_map[acc_id]
                future = executor.submit(
                    run_aws_worker_process,
                    account=account,
                    export_name=export_info['export_name'],
                    region=export_info.get('region', 'us-east-1'),
                    output_dir=output_dir
                )
                futures[future] = f"AWS - {account.get('name', acc_id)}"
        
        # Iterate over Azure
        for azure_info in cost_config.get('azure', []):
            export_name = azure_info.get('export_name')
            
            if 'billing_id' in azure_info:
                scope_type = 'billing'
                scope_id = azure_info['billing_id']
            elif 'subscription_id' in azure_info:
                scope_type = 'subscription'
                scope_id = azure_info['subscription_id']
            else:
                log.error("Azure configuration has to have either 'billing_id' or 'subscription_id' scope.")
                continue
            
            future = executor.submit(
                run_azure_worker_process,
                scope_type=scope_type,
                scope_id=scope_id,
                export_name=export_name,
                output_dir=output_dir
            )
            futures[future] = f"Azure - {scope_type} - {scope_id[:8]}"

        for f in as_completed(futures):
            task_name = futures[f]
            try:
                files, task_success = f.result()
                if files:
                    all_downloaded_files.extend(files)
                if not task_success:
                    success = False
            except Exception as exc:
                log.error(f"Critical error during {task_name}: {exc}")
                success = False

    log.info(f"Download finished, total files: {len(all_downloaded_files)}")
    
    if all_downloaded_files:
        print(all_downloaded_files)

    if not success:
        sys.exit(1)

if __name__ == "__main__":
    run_cost_downloads()