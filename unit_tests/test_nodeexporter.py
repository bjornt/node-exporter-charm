import argparse
import io
import os

from charms.layer import basic
import charms.reactive
from charms.reactive.helpers import data_changed

from charmhelpers.core import hookenv
from charmtest import CharmTest


class Snap:

    name = "snap"

    def __init__(self):
        self.snaps = {}

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("command")
        parser.add_argument("snap_name")
        args = parser.parse_args(proc_args["args"][1:])
        if args.command == "install":
            self.snaps[args.snap_name] = {}
        else:
            raise AssertionError("Command not implemented: " + args.command)
        return {}


class ResourceGet:

    name = "resource-get"

    def __call__(self, proc_args):
        return {"stdout": io.BytesIO(b"")}


class JujuReactiveControl:

    def __init__(self):
        self.applications = {}

    def deploy(self, unit_name):
        application = unit_name.split("/", 1)[0]
        unit_info = {"state": "deployed"}
        self.applications.setdefault(application, {})[unit_name] = unit_info

    def start(self, unit_name):
        application = unit_name.split("/", 1)[0]
        unit = self.applications[application][unit_name]
        assert unit["state"] == "deployed"
        os.environ["JUJU_HOOK_NAME"] = "install"
        charms.reactive.main()
        os.environ["JUJU_HOOK_NAME"] = "start"
        charms.reactive.main()
        unit["state"] = "started"


class FooTest(CharmTest):

    def setUp(self):
        super().setUp()
        self._init_reactive()
        self._init_snap_layer()
        self._init_fake_juju()

    def _init_fake_juju(self):
        tools_dir = "/var/lib/juju/tools/machine-0"
        os.makedirs(tools_dir)
        jujud_path = os.path.join(tools_dir, "jujud")
        with open(jujud_path, "w") as jujud:
            jujud.write("#!/bin/sh\necho 2.0.3\n")
        os.chmod(jujud_path, 0o755)

        self.resource_get = ResourceGet()
        self.fakes.processes.add(self.resource_get)

        self.fakes.juju.control = JujuReactiveControl()

    def _init_reactive(self):
        basic.init_config_states()
        code_dir = os.getcwd()
        charm_dir = hookenv.charm_dir()
        for sub_path in ["reactive", "layer.yaml"]:
            source = os.path.join(code_dir, sub_path)
            target = os.path.join(charm_dir, sub_path)
            os.symlink(source, target)

    def _init_snap_layer(self):
        hookenv.config()["snap_proxy"] = ""
        data_changed("snap.proxy", "")
        self.snap = Snap()
        self.fakes.processes.add(self.snap)

    def test_install_snap(self):
        self.fakes.juju.control.deploy("prometheus-node-exporter")
        self.fakes.juju.control.start("prometheus-node-exporter")
        self.assertEqual(
            ["bjornt-prometheus-node-exporter"], list(self.snap.snaps.keys()))
