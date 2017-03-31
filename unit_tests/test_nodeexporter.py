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


class Apt:

    name = "apt"

    def __init__(self):
        self.installs = []

    def __call__(self, proc_args):
        parser = argparse.ArgumentParser()
        parser.add_argument("command")
        parser.add_argument("package_name")
        non_flag_args = [
            arg for arg in proc_args["args"][1:] if not arg.startswith("-")]
        args = parser.parse_args(non_flag_args)
        if args.command == "install":
            self.installs.append(args.package_name)
        else:
            raise AssertionError("Command not implemented: " + args.command)
        return {}


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
        # Don't add help, since it prints to stdout and raises
        # SystemExit.
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("-r", "--relation")
        parser.add_argument("--file")
        parser.add_argument("--help", action="store_true")
        args = parser.parse_args(proc_args["args"][1:])
        if args.help:
            # We should return BytesIO here, but since fixture's
            # FakeProcess doesn't respect universal_newlines, we have to
            # return a string, since that's what charmhelpers expect.
            # https://github.com/testing-cabal/fixtures/issues/37
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
    """Simulate a Juju model for a reactive charm.

    The application under test will be deployed after model creation.
    After that You can deploy and relate other applications like you
    would with a normal Juju model. A state machine keeps track on when
    the different hooks would be fired.

    For example:

        >>> os.environ["JUJU_UNIT_NAME"] = "wordpress/0"
        >>> model = JujuRectiveModel()
        >>> model.deploy(["mysql", "haproxy"])
        >>> model.relate("db", "mysql")
        >>> model.relate("website", "haproxy")

        >>> model.start("wordpress") # Runs install hook
        >>> model.start("haproxy")  # Runs website-relation-{joined,changed}
        >>> model.start("mysql")  # Runs db-relation-{joined,changed} hooks

    The deploy() and relate() sets up the model, but no hooks will be
    called, since no applications are running yet. The interesting part
    comes when you start the different applications. When the
    application under test is started, its install hook will fire. Then
    as you start the applications that have relations, their relations
    joined/changed hooks will fire.

    This gives you control over the hook ordering, so you can test what
    happens if haproxy starts before mysql or vice versa.

    The hooks are run using charms.reactive.main(), just like its done
    if you use the reactive framework in your charm.

    If you're testing a subordinate charm, it's worth noting that you
    can't start a subordinate application directly. You have to relate
    it to a principal application, which you can start. Example:

        >>> os.environ["JUJU_UNIT_NAME"] = "nrpe/0"
        >>> model = JujuRectiveModel()
        >>> model.deploy(["mysql"])
        >>> model.relate("general-info", "mysql")

        >>> model.start("mysql")

    When mysql is started, the nrpe install hook will fire, as well as
    the general-info-relation-{joined,changed} hooks.

    Only one unit per application is supported.

    @ivar unit_name: The name of the Juju unit under test.
    @ivar application: The name of the Juju application the unit under
        test is part of.
    @ivar local_unit: The state of the unit under test.
    @ivar relations: The state of the relations for the unit under test.
    """

    def __init__(self, unit_name):
        charm_dir = hookenv.charm_dir()
        with open(os.path.join(charm_dir, "metadata.yaml"), "r") as meta_file:
            self.meta = yaml.safe_load(meta_file.read())
        self.unit_name = hookenv.local_unit()
        self.application = unit_name.split("/", 1)[0]
        self.applications = {}
        self.deploy(
            [self.application],
            subordinate=self.meta.get("subordinate", False))
        self.local_unit = self.applications[self.application][0]
        self.local_unit["data"] = {"private-address": "10.1.2.3"}
        self.relations = {}

    def deploy(self, applications, subordinate=False):
        """Deploy one unit each of the given applications.

        The unit won't be started. They have to be started explicitly
        using start().

        @param applications: List of application names that should be deployed.
        @param subordinate: Whether the application is a subordinate.
        """
        for application in applications:
            unit_info = {
                "state": "deployed", "subordinate": subordinate,
                "application": application, "name": application + "/0"}
            self.applications.setdefault(application, []).append(unit_info)

    def start(self, application):
        """Start the application.

        The unit that was deployed for the given application will be
        marked as started. Any install or relation hooks that needs be
        fired as a consequence will be fired.

        If the application is a subordinate, it can't be started
        explicitly. It has to be related to a started principal
        application instead.

        @param application: The name of the application that should be
            started.
        """
        unit = self.applications[application][0]
        assert not unit["subordinate"], (
            "Can't manually start subordinate units.")
        self._transition_unit(unit, "started")

    def relate(self, relation_name, application):
        """Relate the application under test to another application.

        @param relation_name: The name of the relation as specified in
            metadata.yaml.
        @param application: The name of the application to relate to.
        """
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
        """Run the given hook for the unit under test.

        The reactive framework will be used to execute the hook.

        The JUJU_HOOK_NAME and JUJU_RELATION environment variables will
        be set during the hook executing.

        @param name: The name of the hook to execute.
        """
        os.environ["JUJU_HOOK_NAME"] = name
        if self._is_relation_hook(name):
            os.environ["JUJU_RELATION"] = name.rsplit("-", 2)[0]
        charms.reactive.main()
        # XXX: Instead of deleting the environment variables, we should
        #      reset them to their original values.
        if self._is_relation_hook(name):
            del os.environ["JUJU_RELATION"]
        del os.environ["JUJU_HOOK_NAME"]

    def _is_relation_hook(self, hook_name):
        """Return whether the hook is a relation hook.

        @param hook_name: The name of the hook.
        """
        for suffix in ["joined", "changed", "departed", "broken"]:
            if hook_name.endswith("-relation-" + suffix):
                return True
        return False

    def _check_relations(self):
        """Check if any relations should be established.

        For principal applications, both applications that are related
        need to started for the relations to be joined and the relation
        hooks to be fired.

        For subordinate applications, only the non-subordinate
        application need to be started for the container relations to be
        joined.  The subordinate application will be started implicitly
        before the relation is marked as joind and the relation hooks
        are fired.

        Relations that are already joined are ignored.
        """
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
        """Transition a unit to the given state.

        Any install or relation hooks that need to be fired as a
        consequence of the state transition will be fired.
        """
        if state == "started" and unit["state"] == "deployed":
            if unit["application"] == self.application:
                self.run_hook("install")
                self.run_hook("start")
        unit["state"] = state
        self._check_relations()

    def _transition_relation(self, relation, state, remote_unit):
        """Transition a relation to the given state.

        Any relation hooks that need to be fired as a consequence of the
        state transition will be fired.
        """
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
        new_modules = set(sys.modules.keys()) - self.loaded_modules
        for module_name in new_modules:
            del sys.modules[module_name]

    def _init_fake_juju(self):
        tools_dir = "/var/lib/juju/tools/machine-0"
        os.makedirs(tools_dir)
        jujud_path = os.path.join(tools_dir, "jujud")
        with open(jujud_path, "w") as jujud:
            jujud.write("#!/bin/sh\necho 2.0.3\n")
        os.chmod(jujud_path, 0o755)

        self.resource_get = ResourceGet()
        self.fakes.processes.add(self.resource_get)

        self.fakes.juju.model = JujuReactiveModel(os.environ["JUJU_UNIT_NAME"])
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
        # We need to change the path passed to _load_module, so that
        # each test will be able to re-import the relation interfaces
        # they need.
        self.useFixture(Overlay(
            "charms.reactive.relations._load_module", self.fakes.fs._generic,
            self.fakes.fs._is_fake_path))

    def _init_snap_layer(self):
        """Add a snap binary, which the snap layer needs."""
        self.snap = Snap()
        self.fakes.processes.add(self.snap)
        self.apt = Apt()
        self.fakes.processes.add(self.apt)
        # The snap layer reloads snapd when proxy settings have change.
        # Make it believe that the proxy is unset and hasn't changed, so
        # that we don't have to mock out the snapd restart.
        hookenv.config()["snap_proxy"] = ""
        data_changed("snap.proxy", "")

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
