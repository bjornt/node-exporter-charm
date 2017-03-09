import argparse
import shutil
import io
import os
import sys

import yaml

from charms.layer import basic
import charms.reactive
from charms.reactive.helpers import data_changed

from charmhelpers.core import hookenv, unitdata
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

    def __init__(self, charm_dir, unit_name):
        with open(os.path.join(charm_dir, "metadata.yaml"), "r") as meta_file:
            self.meta = yaml.safe_load(meta_file.read())
        self.unit_name = unit_name
        self.application = unit_name.split("/", 1)[0]
        self.applications = {}
        self.local_unit = self.deploy(
            self.application, subordinate=self.meta.get("subordinate", False))
        self.relations = {}

    def deploy(self, application, subordinate=False):
        unit_info = {
            "state": "deployed", "subordinate": subordinate,
            "application": application}
        self.applications.setdefault(application, []).append(unit_info)
        return unit_info

    def start(self, application):
        unit = self.applications[application][0]
        assert not unit["subordinate"], (
            "Can't manually start subordinate units.")
        self._transition_unit(unit, "started")

    def relate(self, relation_name, application):
        for relation_type in ["provides", "requires"]:
            defined_relations = self.meta.get(relation_type, {})
            if relation_name in defined_relations:
                relation = dict(defined_relations[relation_name])
                break
        else:
            raise AssertionError("No such relation defined: " + relation_name)
        relation["state"] = "waiting"
        relation["name"] = relation_name
        relation["application"] = application
        self.relations[relation_name] = relation
        self._check_relations()

    def run_hook(self, name):
        os.environ["JUJU_HOOK_NAME"] = name
        charms.reactive.main()

    def _check_relations(self):
        for relation_name, relation in self.relations.items():
            if relation["state"] != "waiting":
                continue
            if relation["application"] not in self.applications:
                continue
            remote_unit = self.applications[relation["application"]][0]
            if relation.get("scope") == "container":
                if (self.local_unit["state"] == "deployed" and
                        remote_unit["state"] == "started"):
                    self._transition_unit(self.local_unit, "started")
                    self._transition_relation(relation, "joined")
                    self.run_hook(relation_name + "-joined")
                    self.run_hook(relation_name + "-changed")

    def _transition_unit(self, unit, state):
        if state == "started" and unit["state"] == "deployed":
            if unit["application"] == self.application:
                self.run_hook("install")
                self.run_hook("start")
        unit["state"] = state
        self._check_relations()

    def _transition_relation(self, relation, state):
        if state == "joined" and relation["state"] == "waiting":
            self.run_hook(relation["name"] + "-joined")
            self.run_hook(relation["name"] + "-changed")


class FooTest(CharmTest):

    def setUp(self):
        super().setUp()
        self._init_reactive()
        self._init_snap_layer()
        self._init_fake_juju()

    def _clean_up_unitdata(self):
        unitdata.kv().close
        unitdata._KV = None
        delattr(charms.reactive, "_snap_registered")

    def _init_fake_juju(self):
        tools_dir = "/var/lib/juju/tools/machine-0"
        os.makedirs(tools_dir)
        jujud_path = os.path.join(tools_dir, "jujud")
        with open(jujud_path, "w") as jujud:
            jujud.write("#!/bin/sh\necho 2.0.3\n")
        os.chmod(jujud_path, 0o755)

        self.resource_get = ResourceGet()
        self.fakes.processes.add(self.resource_get)

        self.fakes.juju.control = JujuReactiveControl(
            os.environ["CHARM_DIR"], os.environ["JUJU_UNIT_NAME"])
        self.addCleanup(self._clean_up_unitdata)

    def _init_reactive(self):
        self.loaded_modules = set(sys.modules.keys())
        basic.init_config_states()
        code_dir = os.getcwd()
        charm_dir = hookenv.charm_dir()
        shutil.copytree(
            os.path.join(code_dir, "reactive"), 
            os.path.join(
                self.fakes.fs.root.path, charm_dir.lstrip("/"), "reactive"))
        for sub_path in ["layer.yaml"]:
            source = os.path.join(code_dir, sub_path)
            target = os.path.join(charm_dir, sub_path)
            os.symlink(source, target)

    def _init_snap_layer(self):
        hookenv.config()["snap_proxy"] = ""
        data_changed("snap.proxy", "")
        self.snap = Snap()
        self.fakes.processes.add(self.snap)

    def test_install_snap(self):
        self.fakes.juju.control.deploy("some-service")
        self.fakes.juju.control.relate("container", "some-service")
        self.fakes.juju.control.start("some-service")
        self.assertEqual(
            ["bjornt-prometheus-node-exporter"], list(self.snap.snaps.keys()))
