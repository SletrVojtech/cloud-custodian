

def azure_policy_crafter(resource: str, metric: str, interval: str = 'PT5M'):
    POLICY_DATA = {
            'name': 'AZ resource loader: ' + resource,
            'resource': resource,
            'filters': [{
                'type': 'metric',
                'metric': metric, 
                'aggregation': 'average', 
                'op': 'ge', 
                'threshold': 0, 
                'timeframe': 1, 
                'interval': interval
                },]
        }
    return POLICY_DATA  