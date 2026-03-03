import argparse
import logging
import sys
import yaml

from c7n.config import Config
from c7n.policy import PolicyCollection
from c7n.loader import PolicyLoader
from c7n.provider import clouds
from c7n.cli import _setup_logger

from typing import List
import tools.data_collector.policy_templates.aws_template as aws_template
import tools.data_collector.policy_templates.azure_template as azure_template

# Import to register the custom execution mode
from policy import InMemoryPullMode


log = logging.getLogger('custodian.commands')

# Policy definition template
"""POLICY_DATA = {
    "policies": [
        {
            'name': 'CPU-util-check',
            'resource': 'azure.vm',
            'filters': [{
                'type': 'metric',
                'metric': 'Percentage CPU', 
                'aggregation': 'average', 
                'op': 'ge', 
                'threshold': 0, 
                'timeframe': 1, 
                'interval': 'PT5M'
                },]
        },{'name': 'AWS-util-check', 'resource': 'aws.ec2', 'filters': [{'type': 'metrics', 'name': 'CPUUtilization', 'days': 0.05, 'period': 60, 'value': 0, 'op': 'ge'}]}
    ]
}"""


def main():
    parser = argparse.ArgumentParser(description="Run Cloud Custodian policies in-memory")
    parser.add_argument('--region', default='all', help='Region to target')
    parser.add_argument("-v", "--verbose", action="count", help="Verbose Logging")
    parser.add_argument("-q", "--quiet", action="count", help="Less logging (repeatable)")
    parser.add_argument("-s", "--output_dir",  default='.log',
                       help="Directory mainly for log outputs.")
    parser.add_argument("-m", "--metrics", default="metrics.yml", help="Metrics configuration file")

    args = parser.parse_args()

    # Configure logging
    _setup_logger(args)

    # Setup configuration options
    options = Config.empty(
        region=args.region,
        output_dir=args.output_dir,  # No output directory needed for in-memory execution
        )

    try:
        with open(args.metrics, 'r') as f:
            metrics_conf = yaml.safe_load(f)

        policies_list = []
        for m in metrics_conf.get('metrics', []):
            if m['provider'] == 'aws':
                policies_list.append(aws_template.aws_policy_crafter(
                    m['resource'], m['metric'], m.get('period', 300)))
            elif m['provider'] == 'azure':
                policies_list.append(azure_template.azure_policy_crafter(
                    m['resource'], m['metric'], m.get('interval', 'PT5M')))

        POLICY_DATA = {
            "policies": policies_list
        }
        # Load policies from internal dictionary
        loader = PolicyLoader(options)
        collection = loader.load_data(POLICY_DATA, "in-memory", validate=True)
    except Exception as e:
        print(f"Error loading policies: {e}")
        sys.exit(1)

    if not collection:
        print("No policies found in configuration.")
        sys.exit(0)

    # Initialize providers and expand regions
    # This logic mirrors c7n.commands._load_policies
    provider_policies = {}
    for p in collection:
        provider_policies.setdefault(p.provider_name, []).append(p)

    policies = []
    for provider_name, p_list in provider_policies.items():
        try:
            provider = clouds[provider_name]()
            p_options = provider.initialize(options)
            # initialize_policies handles things like 'region: all' expansion
            policies.extend(provider.initialize_policies(
                PolicyCollection(p_list, p_options), p_options))
        except Exception as e:
            print(f"Error initializing provider {provider_name}: {e}")

    errored_policies: List[str] = []
    exit_code = 0

    # Execute policies similarly to c7n.commands.run
    for policy in policies:
        # Force the execution mode to 'in-memory-pull' if it's currently 'pull' (default)
        # This allows using standard policy files without modification
        if policy.data.get('mode', {}).get('type', 'pull') == 'pull':
            policy.data['mode'] = {'type': 'in-memory-pull'}

        # Expand variables (e.g. {account_id}, {region}) in the policy
        policy.expand_variables(policy.get_variables())

        print(f"Running policy: {policy.name} (resource: {p.resource_type})")
        try:
            # Execute the policy
            # resources = p.run()
            resources = policy()

            print(f"  Matched {len(resources)} resources")
            for r in resources:
                print(r)
        except Exception as e:
            exit_code = 2
            errored_policies.append(policy.name)
            if options.debug:
                raise
            log.exception(
                "Error while executing policy %s, continuing" % (
                    policy.name))
    if exit_code != 0:
        log.error("The following policies had errors while executing\n - %s" % (
            "\n - ".join(errored_policies)))
        sys.exit(exit_code)


if __name__ == '__main__':
    main()