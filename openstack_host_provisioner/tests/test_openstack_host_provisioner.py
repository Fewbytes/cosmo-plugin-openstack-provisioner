import argparse
import logging
import random
import string
import inspect
import time
import nova_config
from openstack_host_provisioner import tasks
from openstack_host_provisioner import monitor

__author__ = 'elip'


class TestClass:

    def setUp(self):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger("test_openstack_host_provisioner")
        self.logger.level = logging.DEBUG
        self.logger.info("setUp called")
        self.nova_client = tasks._init_client(region=nova_config.region_name)
        self.name_prefix = 'cosmo_test_openstack_host_provisioner_{0}_'.format(self._id_generator(3))
        self.logger.info("setup")
        self.timeout = 120

    def tearDown(self):
        self.logger.info("tearDown called")
        servers_list = self.nova_client.servers.list()
        for server in servers_list:
            if server.name.startswith(self.name_prefix):
                self.logger.info("Deleting server with name " + server.name)
                try:
                    server.delete()
                except Exception:
                    self.logger.warning("Failed to delete server with name " + server.name)
            else:
                self.logger.info("NOT deleting server with name " + server.name)

    def _provision(self, name):
        # Only used once but will probably be reused in future
        self.logger.info("Provisioning server with name " + name)
        tasks.provision(__cloudify_id=name, nova_config={
            'region': nova_config.region_name,
            'instance': {
                'image': nova_config.image_id,
                'flavor': nova_config.flavor_id,
                'key_name': nova_config.key_name,
            }
        })
        self._wait_for_machine_state(name, u'running')

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

        self.logger.info("Running " + str(inspect.stack()[0][3] + " : "))
        name = self.name_prefix + "test_provision_terminate"

        self._provision(name)

        self.logger.info("Terminating server with name " + name)
        tasks.terminate(__cloudify_id=name, nova_config={
            'region': nova_config.region_name
        })

        expire = time.time() + 10
        while time.time() < expire:
            self.logger.info("Querying server by name " + name)
            try:
                tasks._get_server_by_name(self.nova_client, name)
                self.logger.info("Server has not yet terminated. sleeping...")
                time.sleep(0.5)
            except Exception:
                self.logger.info("Server has terminated. All good")
                return
        raise Exception("Server with name " + name + " was not terminated after 10 seconds")

    def _id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    def _wait_for_machine_state(self, name, expected_state):        

        deadline = time.time() + self.timeout
        host_name_tag = 'name={0}'.format(name)
        m = None
        logger = self.logger

        class ReporterWaitingForMachineStatus():
            def report(self, e):
                # FIXME: timeout will not work if there are no machines to report
                if time.time() > deadline:
                    raise RuntimeError("Timed out waiting for machine {0} to achieve status {1}")
                if host_name_tag in e['tags']:
                    if e['state'] == expected_state:
                        logger.info('machine {0} reached expected machine state {1}'.format(name,expected_state))
                        m.stop()
                    else:
                        logger.info('waiting for machine {0} expected state:{1} current state:{2}'.format(name, expected_state, e['state']))

            def stop(self):
                pass

        r = ReporterWaitingForMachineStatus()
        args = argparse.Namespace(monitor_interval=3, region_name=nova_config.region_name)
        m = monitor.OpenstackStatusMonitor(r, args)
        m.start()
