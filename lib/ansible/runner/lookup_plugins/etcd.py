# (c) 2013, Jan-Piet Mens <jpmens(at)gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

from ansible import utils
import os
import urllib2
try:
    import json
except ImportError:
    import simplejson as json


ETCD_API_VERSION_V1 = "v1"
ETCD_API_VERSION_V2 = "v2"

ETCD_HOST = "http://127.0.0.1:4001"
ETCD_API_VERSION = ETCD_API_VERSION_V2


def fill_keys(d, keys):
    fill = d
    for key in keys:
        if key not in fill:
            fill[key] = {}
        fill = fill[key]
    return d


class EtcdNode(object):
    def __init__(self, node, recurse=True):
        self.recurse = recurse
        self.leaf = not node.get('dir', False)
        self.children = node.get('nodes', [])
        self.key = node['key']
        self.key_parts = node['key'].lstrip('/').split('/')
        self.key_name = self.key_parts[-1]
        self.value = node.get('value', None)

    def accumulate(self, accumulator):
        if accumulator is None: return

        accumulator = fill_keys(accumulator, self.key_parts)
        level = reduce(lambda d, k: d[k], self.key_parts[:-1], accumulator)
        level[self.key_name] = self.value

    def walk(self, accumulator=None):
        if self.children:
            for node in self.children:
                node = EtcdNode(node)
                if self.recurse:
                    if not node.children:
                        node.accumulate(accumulator)
                        yield node

                    for child in node.children:
                        child = EtcdNode(child)
                        for node in child.walk(accumulator):
                            yield node
                else:
                    node.accumulate(accumulator)
                    yield node
        else:
            self.accumulate(accumulator)
            yield self
            return
        return


class EtcdLookup(object):
    def __init__(self, version=ETCD_API_VERSION, host=ETCD_HOST):
        self.base_url = "{}/{}/keys".format(host, version)
        self.api_version = version

    def walk_node(self, node, recursive):
        accumulator = {}
        tree = EtcdNode(node, recursive)
        for node in tree.walk(accumulator):
            pass

        level = reduce(lambda d, k: d[k], tree.key_parts[:-1], accumulator)
        return level[tree.key_name]

    def value_from_node(self, node, recursive):
        value = self.walk_node(node, recursive)
        if isinstance(value, dict) and not any(value.values()):
            value = value.keys()
        return value

    def node_from_results(self, key, results):
        if self.api_version == ETCD_API_VERSION_V1:
            if isinstance(results, list):
                node = {
                    "dir": True,
                    "nodes": results,
                    "key": key,
                }
            else:
                node = results
        else:
            node = results['node']
        return node

    def get(self, key, recursive=True):
        url = "{}/{}".format(self.base_url, key.lstrip('/'))
        if recursive and ETCD_API_VERSION_V2:
            url = "{}?recursive=true".format(url)

        results_json = None
        try:
            response = urllib2.urlopen(url)
            results_json = response.read()
        except Exception as e:
            return ""

        try:
            results = json.loads(results_json)
            node = self.node_from_results(key, results)
            value = self.value_from_node(node, recursive)
        except Exception as e:
            raise

        return value


class LookupModule(object):

    def __init__(self, basedir=None, **kwargs):
        self.basedir = basedir
        self.etcd = EtcdLookup()

    def run(self, terms, inject=None, **kwargs):
        terms = utils.listify_lookup_plugin_terms(terms, self.basedir, inject)

        ret = []
        if isinstance(terms, dict):
            for item_key, key in terms.items():
                value = self.etcd.get(key)
                ret.append({'key': item_key, 'value': value})
        else:
            for key in terms:
                value = self.etcd.get(key)
                if isinstance(value, dict):
                    for value_key, value_node in value.iteritems():
                        ret.append({'key': value_key, 'value': value_node})
                else:
                    ret.append(value)

        return ret
