#!/usr/bin/env python
# vim: ts=4 sw=4 et

import argparse
import inspect
import time
import unittest
from openstack_host_provisioner import tasks
from openstack_host_provisioner import monitor

from openstack_neutron_network_provisioner import tasks as net_tasks
from openstack_neutron_subnet_provisioner import tasks as subnet_tasks

# import cosmo_plugin_common as plugin_common

import cosmo_plugin_openstack_common as os_common

__author__ = 'elip'

TEST_WITH_N_NETS = 3


class OpenstackProvisionerTestCase(os_common.TestCase):

    def _provision(self, name, management_network_name):
        # Only used once but will probably be reused in future
        nova_client = os_common.NovaClient().get(region=tests_config['region'])
        self.logger.info("Provisioning server with name " + name)
        __cloudify_id = "{0}_cloudify_id".format(name)
        tasks.provision(__cloudify_id=__cloudify_id, nova_config={
            'region': tests_config['region'],
            'instance': {
                'name': name,
                'image': nova_client.images.find(name=tests_config['image_name']).id,
                'flavor': tests_config['flavor_id'],
                'key_name': tests_config['key_name'],
            }
        }, management_network_name=management_network_name)
        self._wait_for_machine_state(__cloudify_id, u'running')

    @unittest.skip("TEMP!")
    def test_provision_terminate(self):
        """
        Test server termination by Nova.

        This test should detect termination by a function similar to
        self._wait_for_machine_state() but uses tasks._get_server_by_name()
        instead.  The reason is that using such function would be very non
        trivial.  OpenstackStatusMonitor currently reports one event per
        existing server. Detecting non-existent server is therefore not a
        trivial task.
        """

        nova_client = self.get_nova_client()

        self.logger.info("Running " + str(inspect.stack()[0][3] + " : "))
        name = self.name_prefix + "provision_terminate"

        self._provision(name, management_network_name=self._create_net(254)['name'])

        self.logger.info("Terminating server with name " + name)
        tasks.terminate(nova_config={
            'region': tests_config['region'],
            'instance': {
                'name': name
            }
        })

        timeout = 10
        expire = time.time() + timeout
        while time.time() < expire:
            self.logger.info("Querying server by name " + name)
            by_name = tasks._get_server_by_name(nova_client, name)
            if not by_name:
                self.logger.info("Server has terminated. All good")
                return
            self.logger.info("Server has not yet terminated. it is in state"
                             " {0} sleeping...".format(by_name.status))
            time.sleep(0.5)
        raise Exception("Server with name " + name + " was not terminated "
                                                     "after {0} seconds"
                                                     .format(timeout))

    def _create_net(self, i):
        nc = self.get_neutron_client()
        name = self.name_prefix + 'net_10_1_' + str(i)
        net = nc.create_network({
            'network': {
                'name': name,
                'admin_state_up': True
            }
        })['network']
        subnet = nc.create_subnet({
            'subnet': {
                'network_id': net['id'],
                'ip_version': 4,
                'cidr': '10.1.' + str(i) + '.0/24',
            }
        })
        return net

    def test_with_subnets(self):
        networks = [self._create_net(i) for i in range(1, TEST_WITH_N_NETS+1)]
        name = self.name_prefix + 'server_with_nics'
        self._provision(name, management_network_name=networks[0]['name'])
        for network in networks[1:]:
            tasks.connect_network(
                {'network': {'name': network['name']}},
                {'nova_config': {'region': tests_config['region'], 'instance': {'name': name}}},
            )
        nc = self.get_nova_client()
        server = nc.servers.find(name=name)
        networks_names = server.networks.keys()
        for network in networks:
            self.assertIn(network['name'], networks_names)
        server.delete()
        # I'm really sorry. Can't cleanup networks because the call
        # is async and the networks are still in use.
        time.sleep(15)


    def _wait_for_machine_state(self, cloudify_id, expected_state):

        deadline = time.time() + self.timeout
        cloudify_id_tag = 'name={0}'.format(cloudify_id)
        m = None
        logger = self.logger

        class ReporterWaitingForMachineStatus():

            def report(self, e):

                # FIXME: timeout will not work if there are no
                # FIXME (cont): machines to report
                if time.time() > deadline:
                    raise RuntimeError("Timed out waiting for machine {0} "
                                       "to achieve status {1}"
                                       .format(cloudify_id, expected_state))
                if cloudify_id_tag in e['tags']:
                    if e['state'] == expected_state:
                        logger.info('machine {0} reached expected machine'
                                    ' state {1}'.format(cloudify_id,
                                                        expected_state))
                        m.stop()
                    else:
                        logger.info(
                            'waiting for machine {0} expected state:{1} '
                            'current state:{2}'.format(
                                cloudify_id,
                                expected_state,
                                e['state']))

            def stop(self):
                pass

        r = ReporterWaitingForMachineStatus()
        args = argparse.Namespace(monitor_interval=3,
                                  region_name=tests_config['region'])
        m = monitor.OpenstackStatusMonitor(r, args)
        m.start()


if __name__ == '__main__':
    tests_config = os_common.TestsConfig().get()
    unittest.main()
