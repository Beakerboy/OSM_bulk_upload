#!/usr/bin/python
# -*- coding: utf-8 -*-
#
#
# This is a python version of
# the bulk_upload script for the 0.6 API.
#
# usage:
#      -i input.osm
#      -u username
#      -p password
#      -c comment for change set
#
# After each change set is sent to the server the id mappings are saved
# in inputfile.osm.db
# Subsequent calls to the script will read in these mappings,
#
# If you change $input.osm between calls to the script (ie different data with
# the same file name) you should delete $input.osm.db
#
# Authors: Steve Singer <ssinger_pg@sympatico.ca>
#          Thomas Wood <grand.edgemaster@gmail.com>
#
# COPYRIGHT
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
import argparse
import xml.etree.cElementTree as ETree
import httplib2
import pickle
import os
import sys
from typing import TypeVar
try:
    import pygraph
except ImportError:
    from graph import graph as pygraph


user_agent = "bulk_upload.py/git Python/" + sys.version.split()[0]

api_host = 'https://api.openstreetmap.org'
#api_host = 'http://api06.dev.openstreetmap.org'
headers = {
    'User-Agent': user_agent,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument( "-i", "--input", required=True, help="read data from input.osm")
    parser.add_argument( "-u", "--user", required=True, help="username")
    parser.add_argument("-p", "--password", required=True, help="password")
    parser.add_argument("-c", "--comment", required=True, help="ChangeSet Comment")
    args = parser.parse_args()

    id_map = IdMap(options.infile + ".db")
    tags = {
        'created_by': user_agent,
        'comment': args.comment
    }
    import_processor = ImportProcessor(args.user, args.password, id_map, tags)
    import_processor.parse(args.input)


class XMLException(Exception):
    pass


class APIError(Exception):
    pass


T = TypeVar('T', bound='ImportProcessor')


class ImportProcessor:
    current_changes = None
    id_map = None

    def __init__(self: T, user: str, password: str, id_map: IdMap, tags: dict = {}) -> None:
        self.httpObj = httplib2.Http()
        self.httpObj.add_credentials(user,password)
        self.id_map = id_map
        self.tags = tags
        self.create_changeset()


    def parse(self: T, infile) -> None:
        relationStore = {}
        relationSort = False
        
        osmData = ETree.parse(infile)
        osmRoot = osmData.getroot()
        if osmRoot.tag != "osm":
            raise XMLException("Input file must be a .osm XML file (JOSM-style)")

        # Add a very loud warning to people who try to force osmChange files through
        for zomgOsmChange in ('add', 'delete', 'modify'):
            for arglh in osmRoot.iter(zomgOsmChange):
                raise XMLException("\nYou are processing an osmChange file with a <osm> root element.\nOSM FILES HOWEVER DO NOT HAVE <%s> ELEMENTS.\nYOU ARE PROBABLY TRYING TO UPLOAD A OSM CHANGE FILE\nDIRECTLY *DON'T DO THIS* IT WILL BREAK THINGS\nON THE SERVER AND TOM HUGHES WILL EAT YOUR FAMILY\n(YES REALLY)" % zomgOsmChange)

        for elem in osmRoot.iter('member'):
            if elem.attrib['type'] == 'relation':
                relationSort = True
                break
        
        for type in ('node','way'):
            for elem in osmRoot.iter(type):
                # If elem.id is already mapped we can skip this object
                #
                id = elem.attrib['id']
                if id in self.id_map[type]:
                    continue
                #
                # If elem contains nodes, ways or relations as a child
                # then the ids need to be remapped.
                if elem.tag == 'way':
                    count=0
                    for child in elem.iter('nd'):
                        count=count + 1
                        if count > 2000:
                            raise XMLException("\nnode count >= 2000 in <%s>\n" % elem.attrib['id'])
                        if 'ref' in child.attrib:
                            old_id = child.attrib['ref']
                            if old_id in id_map['node']:
                                child.attrib['ref'] = self.id_map['node'][old_id]
                
                self.add_to_changeset(elem)

        for elem in osmRoot.iter('relation'):
            if relationSort:
                relationStore[elem.attrib['id']] = elem
            else:
                if elem.attrib['id'] in self.id_map['relation']:
                    continue
                else:
                    self.update_relation_member_ids(elem)
                    self.add_to_changeset(elem)

        if relationSort:
            gr = pygraph.digraph()
            gr.add_nodes(relationStore.keys())
            for id in relationStore:
                for child in relationStore[id].iter('member'):
                    if child.attrib['type'] == 'relation':
                        gr.add_edge(id, child.attrib['ref'])

            # Tree is unconnected, hook them all up to a root
            gr.add_node('root')
            for item in gr.node_incidence.iteritems():
                if not item[1]:
                    gr.add_edge('root', item[0])
            for relation in gr.traversal('root', 'post'):
                if relation == 'root': continue
                r = relationStore[relation]
                if r.attrib['id'] in self.id_map['relation']: continue
                self.update_relation_member_ids(r)
                self.add_to_changeset(r)

        self.current_changeset.close() # (uploads any remaining diffset changes)

    def update_relation_member_ids(self: T, elem: ETree.Element) -> None:
        for child in elem.iter('member'):
            if 'ref' in child.attrib:
                old_id = child.attrib['ref']
                old_id_type = child.attrib['type']
                if old_id in self.id_map[old_id_type]:
                    child.attrib['ref'] = self.id_map[old_id_type][old_id]

    def create_changeset(self: T) -> None:
        self.current_changeset = Changeset(tags=self.tags, id_map=self.id_map, httpObj=self.httpObj)

    def add_to_changeset(self: T, elem) -> None:
        if 'action' in elem.attrib:
            action = elem.attrib['action']
        else:
            action = 'create'

        try:
            self.current_changeset.add_change(action, elem)
        except ChangesetClosed:
            self.create_changeset()
            self.current_changeset.add_change(action, elem)


class IdMap:
    # Default IdMap class, using a Pickle backend, this can be extended
    # - if ids in other files need replacing, for example
    id_map = {'node':{}, 'way':{}, 'relation':{}}

    def __init__(self, filename='') -> None:
        self.filename = filename
        self.load()

    def __getitem__(self, item):
        return self.id_map[item]

    def load(self) -> None:
        try:
            if os.stat(self.filename):
                f=open(self.filename, "rb")
                self.id_map = pickle.load(f)
                f.close()
        except:
            pass

    def save(self) -> None:
        f = open(self.filename + ".tmp", "wb")
        pickle.dump(self.id_map, f)
        f.close()
        try:
            os.remove(self.filename)
        except os.error:
            pass
        os.rename(self.filename + ".tmp", self.filename)


class ChangesetClosed(Exception):
    pass


T = Typevar('T', bound='Changeset')


class Changeset:
    id = None
    tags = {}
    currentDiffSet = None
    opened = False
    closed = False
    
    itemcount = 0

    def __init__(self: T, tags: dict = {}, id_map: IdMap: IdMap, httpObj=None) -> None:
        self.id = None
        self.tags = tags
        self.id_map = id_map
        self.httpObj = httpObj
        self.item_limit = 50000
        self.create_diff_set()

    def open(self) -> None:
        createReq = ETree.Element('osm', version="0.6")
        change = ETree.SubElement(createReq, 'changeset')
        for tag in self.tags:
            ETree.SubElement(change, 'tag', k=tag, v=self.tags[tag])
        
        xml = ETree.tostring(createReq)
        resp,content = self.httpObj.request(api_host +
            '/api/0.6/changeset/create','PUT',xml,headers=headers)
        if resp.status != 200:
            raise APIError('Error creating changeset:' + str(resp.status))
        self.id = content.decode("utf-8")
        print("Created changeset: " + str(self.id))
        self.opened = True

    def close(self: T) -> None:
        if not self.opened:
            return
        self.currentDiffSet.upload()
        
        resp, content = self.httpObj.request(
            api_host + '/api/0.6/changeset/' + self.id + '/close',
            'PUT',
            headers=headers
        )
        if resp.status != 200:
            print("Error closing changeset " + str(self.id) + ":" + str(resp.status))
        print("Closed changeset: " + str(self.id))
        self.closed = True

    def create_diff_set(self: T) -> None:
        self.currentDiffSet = DiffSet(self, self.id_map, self.httpObj)

    def add_change(self, action: str, item) -> None:
        if not self.opened:
            self.open() # So that a changeset is only opened when required.
        if self.closed:
            raise ChangesetClosed
        item.attrib['changeset']=self.id
        try:
            self.currentDiffSet.add_change(action, item)
        except DiffSetClosed:
            self.create_diff_set()
            self.currentDiffSet.add_change(action, item)
        
        self.itemcount += 1
        if self.itemcount >= self.item_limit:
            self.currentDiffSet.upload()
            self.close()


class DiffSetClosed(Exception):
    pass


T1 = Typevar('T1', bound='DiffSet')


class DiffSet:
    itemcount = 0
    closed = False
    
    def __init__(self: T1, changeset: Changeset, id_map: IdMap, httpObj):
        self.elems = {
            'create': ETree.Element('create'),
            'modify': ETree.Element('modify'),
            'delete': ETree.Element('delete')
        }
        self.changeset = changeset
        self.id_map = id_map
        self.httpObj = httpObj
        self.item_limit = 1000

    def __getitem__(self: T1, item: ETree.Element):
        return self.elems[item]

    def add_change(self: T1, action: str, item: ETree.Element) -> None:
        if self.closed:
            raise DiffSetClosed
        self[action].append(item)

        self.itemcount += 1
        if self.itemcount >= self.item_limit:
            self.upload()

    def upload(self: T1) -> None:
        if self.itemcount > 0 and not self.closed:
            xml = ETree.Element('osmChange')
            for elem in self.elems.values():
                xml.append(elem)
            print("Uploading to changeset " + str(self.changeset.id))

            xmlstr = ETree.tostring(xml)

            id = self.changeset.id
            resp, content = self.httpObj.request(
                api_host + '/api/0.6/changeset/' + id + '/upload',
                'POST',
                xmlstr,
                headers=headers
            )
            if resp.status != 200:
                print("Error uploading changeset:" + str(resp.status))
                print(content.decode("utf-8"))
                exit(-1)
            else:
                self.process_result(content)
                self.id_map.save()
                self.closed = True

    def process_result(self: T1, content: str) -> None:
        '''
        Uploading a diffset returns a <diffResult> containing elements
        that map the old id to the new id
        Process them.
        '''
        diff_result = ETree.fromstring(content)
        for child in list(diff_result):
            id_type = child.tag
            old_id = child.attrib['old_id']
            if 'new_id' in child.attrib:
                new_id = child.attrib['new_id']
                self.id_map[id_type][old_id] = new_id
            else:
                # (Object deleted)
                self.id_map[id_type][old_id] = old_id


if __name__ == "__main__":
    main()
