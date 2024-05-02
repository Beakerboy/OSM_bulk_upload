# Copyright (c) 2007-2009 Pedro Matiello <pmatiello@gmail.com>
#                         Christian Muise <christian.muise@gmail.com>
#                         Johannes Reinhardt <jreinhardt@ist-dein-freund.de>
#                         Nathan Davis <davisn90210@gmail.com>
#                         Zsolt Haraszti <zsolt@drawwell.net>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# Code taken from python-graph http://code.google.com/p/python-graph/

from typing import Any, Generator, TypeVar


T= TypeVar('T', bound='Digraph')


class GraphError(RuntimeError):
    """
    A base-class for the various kinds of errors that occur in the the python-graph class.
    """
    pass

class AdditionError(GraphError):
    """
    This error is raised when trying to add a node already added to the graph or digraph.
    """
    pass

class Digraph (object):
    """
    Digraph class.

    Digraphs are built of nodes and directed edges.

    @sort: __init__, __getitem__, __iter__, __len__, __str__, add_edge, add_node,
    add_nodes, traversal
    """

    def __str__(self: T) -> str:
        """
        Return a string representing the digraph when requested by str() (or print).

        @rtype:  string
        @return: String representing the graph.
        """
        return "<graph object " + str(self.nodes()) + " " + str(self.edges()) + ">"


    def __len__(self: T)-> int:
        """
        Return the order of the digraph when requested by len().

        @rtype:  number
        @return: Size of the graph.
        """
        return len(self.node_neighbors)


    def __iter__(self: T):
        """
        Return a iterator passing through all nodes in the digraph.

        @rtype:  iterator
        @return: Iterator passing through all nodes in the digraph.
        """
        for each in self.node_neighbors.iterkeys():
            yield each


    def __getitem__(self: T, node):
        """
        Return a iterator passing through all neighbors of the given node.

        @rtype:  iterator
        @return: Iterator passing through all neighbors of the given node.
        """
        for each in self.node_neighbors[node]:
            yield each

    def __init__(self: T) -> None:
        """
        Initialize a digraph.
        """
        self.node_neighbors = {}     # Pairing: Node -> Neighbors
        self.edge_properties = {}    # Pairing: Edge -> (Label, Weight)
        self.node_incidence = {}     # Pairing: Node -> Incident nodes
        self.node_attr = {}          # Pairing: Node -> Attributes
        self.edge_attr = {}          # Pairing: Edge -> Attributes

    def add_node(self: T, node: Any, attrs = []) -> None:
        """
        Add given node to the graph.

        @attention: While nodes can be of any type, it's strongly recommended to use only
        numbers and single-line strings as node identifiers if you intend to use write().

        @type  node: node
        @param node: Node identifier.

        @type  attrs: list
        @param attrs: List of node attributes specified as (attribute, value) tuples.
        """
        if (node not in self.node_neighbors):
            self.node_neighbors[node] = []
            self.node_incidence[node] = []
            self.node_attr[node] = attrs
        else:
            raise AdditionError


    def add_nodes(self: T, nodelist) -> None:
        """
        Add given nodes to the graph.

        @attention: While nodes can be of any type, it's strongly recommended to use only
        numbers and single-line strings as node identifiers if you intend to use write().

        @type  nodelist: list
        @param nodelist: List of nodes to be added to the graph.
        """
        for each in nodelist:
            self.add_node(each)

    def add_edge(self: T, u: Any, v: Any, wt: int=1, label: str='', attrs=[]) -> None:
        """
        Add an directed edge (u,v) to the graph connecting nodes u to v.

        @type  u: node
        @param u: One node.

        @type  v: node
        @param v: Other node.

        @type  wt: number
        @param wt: Edge weight.

        @type  label: string
        @param label: Edge label.

        @type  attrs: list
        @param attrs: List of node attributes specified as (attribute, value) tuples.
        """
        if (v not in self.node_neighbors[u]):
            self.node_neighbors[u].append(v)
            self.node_incidence[v].append(u)
            self.edge_properties[(u, v)] = [label, wt]
            self.edge_attr[(u, v)] = attrs

    def traversal(self: T, node: Any, order: str) -> Generator[Any]:
        """
        Graph traversal iterator.

        @type  graph: graph, digraph
        @param graph: Graph.

        @type  node: node
        @param node: Node.

        @type  order: string
        @param order: traversal ordering. Possible values are:
            2. 'pre' - Preordering (default)
            1. 'post' - Postordering

        @rtype:  iterator
        @return: Traversal iterator.
        """
        visited = {}
        if (order == 'pre'):
            pre = 1
            post = 0
        elif (order == 'post'):
            pre = 0
            post = 1
    
        for each in self._dfs(visited, node, pre, post):
            yield each

    def _dfs(self: T, visited: dict, node: Any, pre: int, post: int
            ) -> Generator[Any]:
        """
        Depth-first search subfunction for traversals.

        @type  graph: graph, digraph
        @param graph: Graph.

        @type  visited: dictionary
        @param visited: List of nodes (visited nodes are marked non-zero).

        @type  node: node
        @param node: Node to be explored by DFS.
        """
        visited[node] = 1
        if (pre):
            yield node
        # Explore recursively the connected component
        for each in self[node]:
            if (each not in visited):
                for other in self._dfs(visited, each, pre, post):
                    yield other
        if (post):
            yield node
