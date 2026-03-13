import time
import logging
import yaml
import sys
import argparse
from botocore.exceptions import ClientError
from c7n.policy import PolicyCollection
from c7n.config import Config
from c7n.loader import PolicyLoader
from c7n_org.utils import environ, account_tags
from c7n_org.cli import get_session, _get_env_creds
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from c7n_org.cli import init, resolve_regions, accounts_iterator
from tools.data_collector.policy_templates.policy_crafter import CrafterFactory
from tools.data_collector.policy import InMemoryPullMode
from tools.data_collector.rabbitmq.connector import RabbitMQClient
from tools.data_collector.rabbitmq.message import IngestionMessage
from tools.data_collector.metrics_collector.message_adapters import AwsAdapter, AzureAdapter



log = logging.getLogger('metrics_collector')

#TODO Split apart
def run_account_in_memory(account, region, policy_data, output_dir, debug=False):
    """
        Worker function based on c7n_org.cli.run_account. 
    """

        # Setup configuration options
    options = Config.empty(
        region=region,
        output_dir=output_dir,  # directory for Custodian policy logs.
        metrics_enabled=False,
        )

    # Load environment variables for given account. 
    env_vars = account_tags(account)
    if account.get('role'):
        if isinstance(account['role'], str):
            options['assume_role'] = account['role']
            options['external_id'] = account.get('external_id')
        else:
            env_vars.update(
                _get_env_creds(account, get_session(account, 'custodian', region), region))

    elif account.get('profile'):
        options['profile'] = account['profile']
    
    success = True
    policy_counts = {}

    # Isolated environment variables
    with environ(**env_vars):
        loader = PolicyLoader(options)
        provider = account.get('provider')
        policy_provider = "*" + provider + "*"
            
        collection = loader.load_data(policy_data, "in-memory").filter(policy_patterns=[policy_provider])
        with RabbitMQClient() as mq_client:
            for policy in collection:
                # Force in-memory-pull
                if policy.data.get('mode', {}).get('type', 'pull') == 'pull':
                    policy.data['mode'] = {'type': 'in-memory-pull'}
                policy.data['regions'] = [region]
                # Expand variables (e.g. {account_id}, {region}) in the policy
                policy.expand_variables(policy.get_variables())
                # Extend policy execution conditions with account information
                policy.conditions.env_vars['account'] = account
                # Variable expansion and non schema validation (not optional)
                policy.expand_variables(policy.get_variables(account.get('vars', {})))
                log.debug(
                    "Running policy:%s account:%s region:%s",
                    policy.name, account['name'], region)
                try:
                    # In memory execution
                    st = time.time()
                    resources = policy() 
                    policy_counts[policy.name] = resources and len(resources) or 0
                    if not resources:
                        return policy_counts, success
                    res_type = policy.resource_type
                    # Parse each returned resource into a RabbitMQ message
                    for raw_resource in resources:
                            if provider == 'aws':
                                adapter = AwsAdapter(
                                    raw_resource, 
                                    account_id=account.get('account_id', 'unknown'),
                                    resource_type=res_type,
                                    region_name=region,
                                    policy_name=policy.name
                                )
                            elif provider == 'azure':
                                adapter = AzureAdapter(
                                    raw_resource,
                                    resource_type=res_type,
                                    policy_name=policy.name
                                )
                            else:
                                log.warning(f"Unknown cloud provider: {provider}")
                                continue
                            
                            metrics_payload = adapter.to_payload()
                            
                            msg = IngestionMessage(
                                source_module="custodian",
                                payload=metrics_payload.model_dump()
                            )
                            
                            mq_client.publish(
                                queue_name="metrics_ingestion", 
                                message=msg.model_dump_json()
                            )

                    log.info(
                        "Ran account:%s region:%s policy:%s matched:%d time:%0.2f",
                        account['name'], region, policy.name, len(resources),
                        time.time() - st)
                    

                except ClientError as e:
                    success = False
                    if e.response['Error']['Code'] == 'AccessDenied':
                        log.warning('Access denied api:%s policy:%s account:%s region:%s',
                                    e.operation_name, policy.name, account['name'], region)
                        return policy_counts, success
                    log.error(
                        "Exception running policy:%s account:%s region:%s error:%s",
                        policy.name, account['name'], region, e)
                    continue
                except Exception as e:
                    success = False
                    log.error(
                        "Exception running policy:%s account:%s region:%s error:%s",
                        policy.name, account['name'], region, e)
                    if not debug:
                        continue
                    import traceback, pdb, sys
                    traceback.print_exc()
                    pdb.post_mortem(sys.exc_info()[-1])
                    raise

    return policy_counts, success


def main():
    parser = argparse.ArgumentParser(description="Run Cloud Custodian policies in-memory")
    parser.add_argument('--region', default='all', help='Region to target')
    parser.add_argument("-v", "--verbose", action="count", help="Verbose Logging")
    parser.add_argument("-q", "--quiet", action="count", help="Less logging (repeatable)")
    parser.add_argument("-s", "--output_dir",  default='.log',
                       help="Directory mainly for log outputs.")
    parser.add_argument("-m", "--metrics", default="metrics.yml", help="Metrics configuration file")

    args = parser.parse_args()



    try:
        with open(args.metrics, 'r') as f:
            metrics_conf = yaml.safe_load(f)

        print(metrics_conf)

        policies_list = []
        for entry in metrics_conf.get('measure', []):
            resource = entry['resource']
            crafter = CrafterFactory.get_crafter(resource)

            for metric in entry['measurement']:
                policies_list.append(crafter.craft(
                    resource=resource,
                    metric=metric['metric'],
                ))

        POLICY_DATA = {
            "policies": policies_list
        }
        print(POLICY_DATA)

    except Exception as e:
        log.error(f"Error loading policies: {e}")
        sys.exit(1)

    # Load c7n-org account config
    accounts_config, _, _ = init(
        config='accounts.yml',
        use=None,
        debug=False,
        verbose=False,
        accounts=None, tags=None, policies=None
    )

    azure_config,_,_ = init(
        config='subscriptions.yml',
        use=None,
        debug=False,
        verbose=False,
        accounts=None, tags=None, policies=None
    )

    accounts_config["subscriptions"] = azure_config.get("subscriptions", ())
    

    worker_count = 4
    policy_counts = Counter()
    success = True

    # Run processes
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        futures = {}
        for account in accounts_iterator(accounts_config):
            for region in resolve_regions(account.get('regions', ['us-east-1']), account):
                
                future = executor.submit(
                    run_account_in_memory,
                    account=account,
                    region=region,
                    policy_data=POLICY_DATA,
                    output_dir=args.output_dir
                )
                futures[future] = (account, region)

        # Collect the results and log failures
        for f in as_completed(futures):
            a, r = futures[f]
            if f.exception():
                log.warning(
                    "Error running policy in %s @ %s exception: %s",
                    a['name'], r, f.exception())
                continue

            account_region_pcounts, account_region_success = f.result()
            for p in account_region_pcounts:
                policy_counts[p] += account_region_pcounts[p]

            if not account_region_success:
                success = False

    log.info("Policy resource counts %s" % policy_counts)

    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main()