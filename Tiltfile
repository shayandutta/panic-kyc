# Local Kubernetes dev loop: `tilt up`, then open the Tilt dashboard.
# Rebuilds and redeploys a service whenever its code changes.

services = {
    'sandbox':              'services/sandbox',
    'verification-service': 'services/verification-service/cmd',
    'bulk-service':         'services/bulk-service/cmd',
    'webhook-service':      'services/webhook-service/cmd',
    'api-gateway':          'services/api-gateway/cmd',
}

for name, path in services.items():
    # Only files this service is built from trigger its rebuild, so editing
    # the webhook service doesn't rebuild the other four.
    service_dir = '/'.join(path.split('/')[:2])
    docker_build(
        'kyc/' + name, '.',
        build_args={'SERVICE_PATH': path},
        only=['go.mod', 'go.sum', 'shared', 'deploy', service_dir],
    )

k8s_yaml(listdir('deploy/k8s'))

for infra in ['postgres', 'mongo', 'redis', 'kafka']:
    k8s_resource(infra, labels=['infra'])

k8s_resource('sandbox', port_forwards='8090:8090', labels=['apps'])
k8s_resource('verification-service', resource_deps=['postgres', 'redis', 'kafka', 'sandbox'], labels=['apps'])
k8s_resource('bulk-service', resource_deps=['mongo', 'verification-service'], labels=['apps'])
k8s_resource('webhook-service', resource_deps=['kafka'], labels=['apps'])
k8s_resource('api-gateway', port_forwards='8080:8080', resource_deps=['redis', 'verification-service', 'bulk-service'], labels=['apps'])
