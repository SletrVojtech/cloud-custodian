
def aws_policy_crafter(resource: str, metric: str, period: int = 300):
    POLICY_DATA = {
        'name': 'AWS resource loader: ' + resource, 
        'resource': resource, 
        'filters': [{
             'type': 'metrics', 
             'name': metric, 
             'days': 0.5, 
             'period': period, 
             'value': 0, 
             'op': 'ge'
             }]
        }
    return POLICY_DATA  