import copy
import inspect
import itertools
import os
import subprocess
import sys
import json

from novaclient.v1_1 import client

# TODO: rewrite, this is bad.
# https://github.com/celery/celery/blob/9c00d0c67c0844e4b62d60839c55a79c64666c93/celery/contrib/methods.py#L45
from celery import task

from celery.utils.log import get_task_logger

__author__ = 'elip'

logger = get_task_logger(__name__)


def get_server(nova_config):

    _fail_on_missing_required_parameters(nova_config, ('region', 'instance'), 'nova_config')
    region = nova_config['region']
    nova_client = _init_client(region=region)
    return _get_server_by_name_or_fail(nova_client, nova_config['instance']['name'])


@task
def provision(__cloudify_id, nova_config, **kwargs):

    """
    Creates a server. Exposes the parameters mentioned in
    http://docs.openstack.org/developer/python-novaclient/api/novaclient.v1_1.servers.html#novaclient.v1_1.servers.ServerManager.create
    Userdata:
        In all cases, note that userdata should not be base64 encoded, novaclient expects it raw.
        The 'userdata' argument under nova.instance can be one of the following:
        1. A string
        2. A hash with 'type: http' and 'url: ...'

    """

    _fail_on_missing_required_parameters(nova_config, ('region', 'instance'), 'nova_config')

    nova_instance = copy.deepcopy(nova_config['instance'])  # For possible changes by _maybe_transform_userdata()
    _maybe_transform_userdata(nova_instance)

    _fail_on_missing_required_parameters(nova_instance, ('name', 'flavor', 'image', 'key_name'),
                                         'nova_config.instance')

    nova_client = _init_client(region=nova_config['region'])

    params_names = inspect.getargspec(nova_client.servers.create).args[1:]  # First parameter is 'self', skipping
    params_default_values = inspect.getargspec(nova_client.servers.create).defaults
    params = dict(itertools.izip(params_names, params_default_values))

    # Fail on unsupported parameters
    for k in nova_instance:
        if k not in params:
            raise ValueError("Parameter with name '{0}' must not be passed to openstack provisioner "
                             "(under host's properties.nova.instance)".format(k))

    for k in params:
        if k in nova_instance:
            params[k] = nova_instance[k]

    if _get_server_by_name(nova_client, nova_config['instance']['name']):
        raise RuntimeError("Can not provision server with name '{0}' because server with such name already exists"
                           .format(__cloudify_id))

    if not params['meta']:
        params['meta'] = dict({})
    params['meta']['cloudify_id'] = __cloudify_id

    logger.info("Asking Nova to create server. Parameters: {0}".format(str(params)))
    logger.debug("Asking Nova to create server. All possible parameters are: {0})".format(','.join(params.keys())))

    nova_client.servers.create(**params)


@task
def terminate(nova_config, **kwargs):
    server = get_server(nova_config)
    server.delete()


@task
def start(nova_config, **kwargs):

    server = get_server(nova_config)

    # ACTIVE - already started
    # BUILD - is building and will start automatically after the build.
    # HP uses 'BUILD(x)' where x is a substatus therfore the startswith usage.

    if server.status == 'ACTIVE' or server.status.startswith('BUILD'):
        start_monitor(nova_config)
        return

    # Rackspace: stop, start, pause, unpause, suspend - not implemented. Maybe other methods too.
    #            Calling reboot() on an instance that is 'SHUTOFF' will start it.

    # SHUTOFF - powered off
    if server.status == 'SHUTOFF':
        server.reboot()
        start_monitor(nova_config)
        return

    raise ValueError("openstack_host_provisioner: Can not start() server in state {0}".format(server.status))


@task
def pause(nova_config, **kwargs):
    server = get_server(nova_config)
    server.pause()


@task
def restart(nova_config, **kwargs):
    server = get_server(nova_config)
    server.restart()


# Not in DSL --- start
@task
def stop(nova_config, **kwargs):
    server = get_server(nova_config)
    server.stop()
# Not in DSL --- end


@task
def start_monitor(nova_config, **kwargs):
    _fail_on_missing_required_parameters(nova_config, ('region',), 'nova_config')
    region = nova_config['region']
    command = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "monitor.py")
    ]
    if region:
        command.append("--region_name={0}".format(region))

    logger.info('starting openstack monitoring [cmd=%s]', command)
    subprocess.Popen(command)


def _init_client(region=None):
    keystone_config = get_keystone_config()
    return client.Client(username=keystone_config['username'],
                         api_key=keystone_config['password'],
                         project_id=keystone_config['tenant_name'],
                         auth_url=keystone_config['auth_url'],
                         region_name=region,
                         http_log_debug=False)


def _get_server_by_name(nova_client, name):
    matching_servers = nova_client.servers.list(True, {'name': name})
    if len(matching_servers) == 0:
        return None
    if len(matching_servers) == 1:
        return matching_servers[0]
    raise RuntimeError("Lookup of server by name failed. There are {0} servers named '{1}'"
                       .format(len(matching_servers), name))


def _get_server_by_name_or_fail(nova_client, name):
    server = _get_server_by_name(nova_client, name)
    if server:
        return server
    raise ValueError("Lookup of server by name failed. Could not find a server with name {0}")


def _fail_on_missing_required_parameters(obj, required_parameters, hint_where):
    for k in required_parameters:
        if k not in obj:
            raise ValueError("Required parameter '{0}' is missing (under host's properties.{1}). "
                             "Required parameters are: {2}".format(k, hint_where, required_parameters))


def get_keystone_config():
    config_path = os.getenv('KEYSTONE_CONFIG_PATH', os.path.expanduser('~/keystone_config.json'))
    with open(config_path, 'r') as f:
        keystone_config = json.loads(f.read())
    return keystone_config


# *** userdata handlig - start ***
userdata_handlers = {}


def userdata_handler(type_):
    def f(x):
        userdata_handlers[type_] = x
        return x
    return f


def _maybe_transform_userdata(nova_config_instance):
    """Allows userdata to be read from a file, etc, not just be a string"""
    if 'userdata' not in nova_config_instance:
        return
    if not isinstance(nova_config_instance['userdata'], dict):
        return
    ud = nova_config_instance['userdata']

    _fail_on_missing_required_parameters(ud, ('type',), 'nova_config.instance.userdata')

    if ud['type'] not in userdata_handlers:
        raise ValueError("Invalid type '{0}' (under host's properties.nova_config.instance.userdata)".format(ud['type']))

    nova_config_instance['userdata'] = userdata_handlers[ud['type']](ud)


@userdata_handler('http')
def ud_http(params):
    """ Fetches userdata using HTTP """
    import requests
    _fail_on_missing_required_parameters(params, ('url',), "nova.instance.userdata when using type 'http'")
    logger.info("Using userdata from URL {0}".format(params['url']))
    return requests.get(params['url']).text
# *** userdata handling - end ***
