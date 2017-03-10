import argparse
import io
import json
import os
import shutil
import sys

import yaml

from charms.layer import basic
import charms.reactive
from charms.reactive.helpers import data_changed

from charmhelpers.core import hookenv, unitdata
from charmtest import CharmTest

from systemfixtures.filesystem import Overlay


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


class UnitGet:

    name = "unit-get"

    def __init__(self, unit_data):
        self.unit_data = unit_data

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("setting")
        parser.add_argument("--format", nargs="?", default="yaml")
        args = parser.parse_args(proc_args["args"][1:])
        if args.setting not in self.unit_data:
            error = 'error: unknown setting "{}"'.format(args.setting)
            return {"error": io.BytesIO(error.encode("utf-8"))}
        converter = json.dumps if args.format == "json" else yaml.dump
        value = converter(self.unit_data[args.setting])
        return {"stdout": io.BytesIO(value.encode("utf-8"))}


class RelationIds:

    name = "relation-ids"

    def __init__(self, relations):
        self.relations = relations

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("name")
        parser.add_argument("--format", nargs="?", default="yaml")
        args = parser.parse_args(proc_args["args"][1:])
        if args.name in self.relations:
            relation_ids = [self.relations[args.name]["id"]]
        else:
            relation_ids = []
        converter = json.dumps if args.format == "json" else yaml.dump
        value = converter(relation_ids)
        return {"stdout": io.BytesIO(value.encode("utf-8"))}


class RelationList:

    name = "relation-list"

    def __init__(self, relations):
        self.relations = relations

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("-r", "--relation")
        parser.add_argument("--format", nargs="?", default="yaml")
        args = parser.parse_args(proc_args["args"][1:])
        for relation in self.relations.values():
            if relation["id"] == args.relation:
                break
        else:
            raise AssertionError("invalid relation id")
        converter = json.dumps if args.format == "json" else yaml.dump
        value = converter(list(relation["units"].keys()))
        return {"stdout": io.BytesIO(value.encode("utf-8"))}


class RelationSet:

    name = "relation-set"

    def __init__(self, relations):
        self.relations = relations

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("-r", "--relation")
        parser.add_argument("--file")
        try:
            args = parser.parse_args(proc_args["args"][1:])
        except SystemExit:
            return {"stdout": io.StringIO("--file")}
        with open(args.file, "r") as settings_file:
            settings = yaml.safe_load(settings_file.read())
        for relation in self.relations.values():
            if relation["id"] == args.relation:
                relation["data"].update(settings)
                break
        else:
            raise AssertionError("invalid relation id")
        return {}


class RelationGet:

    name = "relation-get"

    def __init__(self, relations):
        self.relations = relations

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("key")
        parser.add_argument("unit")
        parser.add_argument("-r", "--relation")
        parser.add_argument("--format", nargs="?", default="yaml")
        args = parser.parse_args(proc_args["args"][1:])
        for relation in self.relations.values():
            if relation["id"] == args.relation:
                break
        else:
            raise AssertionError("invalid relation id")
        data = relation["units"][args.unit]
        value = data if args.key == "-" else data[args.key]
        converter = json.dumps if args.format == "json" else yaml.dump
        value = converter(value)
        return {"stdout": io.BytesIO(value.encode("utf-8"))}


class JujuReactiveControl:

    def __init__(self, charm_dir, unit_name):
        with open(os.path.join(charm_dir, "metadata.yaml"), "r") as meta_file:
            self.meta = yaml.safe_load(meta_file.read())
        self.unit_name = unit_name
        self.application = unit_name.split("/", 1)[0]
        self.applications = {}
        self.deploy(
            [self.application],
            subordinate=self.meta.get("subordinate", False))
        self.local_unit = self.applications[self.application][0]
        self.local_unit["data"] = {"private-address": "10.1.2.3"}
        self.relations = {}

    def deploy(self, applications, subordinate=False):
        for application in applications:
            unit_info = {
                "state": "deployed", "subordinate": subordinate,
                "application": application, "name": application + "/0"}
            self.applications.setdefault(application, []).append(unit_info)

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
        relation["id"] = relation_name + ":1"
        relation["data"] = {}
        relation["units"] = {}
        relation["state"] = "waiting"
        relation["name"] = relation_name
        relation["application"] = application
        self.relations[relation_name] = relation
        self._check_relations()

    def run_hook(self, name):
        os.environ["JUJU_HOOK_NAME"] = name
        if self._is_relation_hook(name):
            os.environ["JUJU_RELATION"] = name.rsplit("-", 2)[0]
        charms.reactive.main()
        if self._is_relation_hook(name):
            del os.environ["JUJU_RELATION"]

    def _is_relation_hook(self, hook_name):
        for suffix in ["joined", "changed", "departed", "broken"]:
            if hook_name.endswith("-relation-" + suffix):
                return True
        return False

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
                    self._transition_relation(relation, "joined", remote_unit)
            else:
                if (self.local_unit["state"] == "started" and
                        remote_unit["state"] == "started"):
                    self._transition_relation(relation, "joined", remote_unit)

    def _transition_unit(self, unit, state):
        if state == "started" and unit["state"] == "deployed":
            if unit["application"] == self.application:
                self.run_hook("install")
                self.run_hook("start")
        unit["state"] = state
        self._check_relations()

    def _transition_relation(self, relation, state, remote_unit):
        if state == "joined" and relation["state"] == "waiting":
            self.run_hook(relation["name"] + "-relation-joined")
            self.run_hook(relation["name"] + "-relation-changed")
            relation["state"] = "joined"
            relation["units"][remote_unit["name"]] = {}


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
        unit_get = UnitGet(self.fakes.juju.control.local_unit["data"])
        self.fakes.processes.add(unit_get)
        relation_ids = RelationIds(self.fakes.juju.control.relations)
        self.fakes.processes.add(relation_ids)
        relation_list = RelationList(self.fakes.juju.control.relations)
        self.fakes.processes.add(relation_list)
        relation_get = RelationGet(self.fakes.juju.control.relations)
        self.fakes.processes.add(relation_get)
        relation_set = RelationSet(self.fakes.juju.control.relations)
        self.fakes.processes.add(relation_set)

    def _init_reactive(self):
        self.loaded_modules = set(sys.modules.keys())
        basic.init_config_states()
        code_dir = os.getcwd()
        charm_dir = hookenv.charm_dir()
        shutil.copytree(
            os.path.join(code_dir, "reactive"),
            os.path.join(
                self.fakes.fs.root.path, charm_dir.lstrip("/"), "reactive"))
        shutil.copytree(
            os.path.join(code_dir, "hooks"),
            os.path.join(
                self.fakes.fs.root.path, charm_dir.lstrip("/"), "hooks"))
        for sub_path in ["layer.yaml"]:
            source = os.path.join(code_dir, sub_path)
            target = os.path.join(charm_dir, sub_path)
            os.symlink(source, target)
        self.useFixture(Overlay(
            "charms.reactive.relations._load_module", self.fakes.fs._generic,
            self.fakes.fs._is_fake_path))

    def _init_snap_layer(self):
        hookenv.config()["snap_proxy"] = ""
        data_changed("snap.proxy", "")
        self.snap = Snap()
        self.fakes.processes.add(self.snap)

    def test_install_snap(self):
        self.fakes.juju.control.deploy(["some-service"])
        self.fakes.juju.control.relate("container", "some-service")

        self.fakes.juju.control.start("some-service")

        self.assertEqual(
            ["bjornt-prometheus-node-exporter"], list(self.snap.snaps.keys()))

    def test_relate_prometheus(self):
        self.fakes.juju.control.deploy(["some-service", "prometheus"])
        self.fakes.juju.control.relate("container", "some-service")
        self.fakes.juju.control.relate("prometheus-client", "prometheus")

        self.fakes.juju.control.start("some-service")
        self.fakes.juju.control.start("prometheus")

        relation = self.fakes.juju.control.relations["prometheus-client"]
        unit_data = self.fakes.juju.control.local_unit["data"]
        self.assertEqual("9100", relation["data"]["port"])
        self.assertEqual(
            unit_data["private-address"], relation["data"]["hostname"])
        self.assertEqual(
            unit_data["private-address"], relation["data"]["private-address"])
        self.assertEqual(
            "some-service/0", relation["data"]["principal-unit"])
