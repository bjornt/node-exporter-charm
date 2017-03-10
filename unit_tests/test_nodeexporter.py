import argparse
import io
import itertools
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
        relation_ids = []
        if args.name in self.relations:
            for relation in self.relations[args.name]:
                relation_ids.append(relation["id"])
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
        relation_name = args.relation.rsplit(":", 1)[0]
        relations = self.relations[relation_name]
        for relation in relations:
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
        relation_name = args.relation.rsplit(":", 1)[0]
        relations = self.relations[relation_name]
        for relation in relations:
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
        relation_name = args.relation.rsplit(":", 1)[0]
        relations = self.relations[relation_name]
        for relation in relations:
            if relation["id"] == args.relation:
                break
        else:
            raise AssertionError("invalid relation id")
        data = relation["units"][args.unit]
        value = data if args.key == "-" else data[args.key]
        converter = json.dumps if args.format == "json" else yaml.dump
        value = converter(value)
        return {"stdout": io.BytesIO(value.encode("utf-8"))}


class JujuReactiveModel:

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
        relations = self.relations.setdefault(relation_name, [])
        relation["id"] = "{}:{}".format(relation_name, len(relations))
        relation["data"] = {}
        relation["units"] = {}
        relation["state"] = "waiting"
        relation["name"] = relation_name
        relation["application"] = application
        self.relations.setdefault(relation_name, []).append(relation)
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
        relations = itertools.chain(*self.relations.values())
        for relation_name, relations in self.relations.items():
            for relation in relations:
                if relation["state"] != "waiting":
                    continue
                if relation["application"] not in self.applications:
                    continue
                remote_unit = self.applications[relation["application"]][0]
                if relation.get("scope") == "container":
                    if (self.local_unit["state"] == "deployed" and
                            remote_unit["state"] == "started"):
                        self._transition_unit(self.local_unit, "started")
                        self._transition_relation(
                            relation, "joined", remote_unit)
                else:
                    if (self.local_unit["state"] == "started" and
                            remote_unit["state"] == "started"):
                        self._transition_relation(
                            relation, "joined", remote_unit)

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

        self.fakes.juju.model = JujuReactiveModel(
            os.environ["CHARM_DIR"], os.environ["JUJU_UNIT_NAME"])
        self.addCleanup(self._clean_up_unitdata)
        unit_get = UnitGet(self.fakes.juju.model.local_unit["data"])
        self.fakes.processes.add(unit_get)
        relation_ids = RelationIds(self.fakes.juju.model.relations)
        self.fakes.processes.add(relation_ids)
        relation_list = RelationList(self.fakes.juju.model.relations)
        self.fakes.processes.add(relation_list)
        relation_get = RelationGet(self.fakes.juju.model.relations)
        self.fakes.processes.add(relation_get)
        relation_set = RelationSet(self.fakes.juju.model.relations)
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
        self.fakes.juju.model.deploy(["mysql"])
        self.fakes.juju.model.relate("container", "mysql")

        self.fakes.juju.model.start("mysql")

        self.assertEqual(
            ["bjornt-prometheus-node-exporter"], list(self.snap.snaps.keys()))

    def test_relate_prometheus(self):
        self.fakes.juju.model.deploy(["mysql", "prometheus"])
        self.fakes.juju.model.relate("container", "mysql")
        self.fakes.juju.model.relate("prometheus-client", "prometheus")

        self.fakes.juju.model.start("mysql")
        self.fakes.juju.model.start("prometheus")

        [relation] = self.fakes.juju.model.relations["prometheus-client"]
        unit_data = self.fakes.juju.model.local_unit["data"]

        self.assertEqual("9100", relation["data"]["port"])
        self.assertEqual(
            unit_data["private-address"], relation["data"]["hostname"])
        self.assertEqual(
            unit_data["private-address"], relation["data"]["private-address"])
        self.assertEqual(
            "mysql/0", relation["data"]["principal-unit"])

    def test_relate_prometheus_multiple(self):
        self.fakes.juju.model.deploy(
            ["mysql", "prometheus1", "prometheus2"])
        self.fakes.juju.model.relate("container", "mysql")
        self.fakes.juju.model.relate("prometheus-client", "prometheus1")
        self.fakes.juju.model.relate("prometheus-client", "prometheus2")

        self.fakes.juju.model.start("mysql")
        self.fakes.juju.model.start("prometheus1")
        self.fakes.juju.model.start("prometheus2")

        relation1, relation2 = self.fakes.juju.model.relations[
            "prometheus-client"]
        self.assertEqual("9100", relation1["data"]["port"])
        self.assertEqual(
            "mysql/0", relation1["data"]["principal-unit"])
        self.assertEqual("9100", relation2["data"]["port"])
        self.assertEqual(
            "mysql/0", relation2["data"]["principal-unit"])
