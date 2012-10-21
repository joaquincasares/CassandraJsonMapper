#!/usr/bin/env python

import ast
import logging
import pycassa
import Queue
import time
import threading

try: import simplejson as json
except: import json

from pycassa.cassandra.ttypes import ConsistencyLevel, NotFoundException, \
                                     InvalidRequestException



def json_format(dict_input):
    """Returns a dict in proper json output. For debugging purposes."""

    return json.dumps(dict_input, sort_keys=True, indent=4)

def string_to_python_type(repr_object):
    """Convert a string to a python type."""

    try:
        # Check if the object is a: string, number, tuple, list,
        # dict, boolean, or None
        return ast.literal_eval(repr_object)
    except:
        # If not, return the string object
        return repr_object

def contruct_arrays_helper(parent_level, this_key):
    """Worker method to construct arrays from modified dicts."""

    this_value = parent_level[this_key]

    # Check if this_value is an end value
    if not isinstance(this_value, dict) and not isinstance(this_value, list):
        # Check and create for empty array placeholders
        if this_value == 'Array:Empty':
            parent_level[this_key] = []

        # Do nothing if this is a proper end value
        return

    # Change names for readability purposes
    this_level = this_value

    if isinstance(this_level, dict):
        next_level = this_level.keys()

        # Check if the child level is really an array
        if next_level[0].startswith('Array:'):
            # Don't perform the sort if we don't have to
            next_level.sort()

            # Pull all elements into a new array
            new_level = []
            for key in next_level:
                new_level.append(this_level[key])

            # Make the parent level now link to this new level,
            # Thereby bypassing the chain with the previous this_level
            parent_level[this_key] = new_level
            this_level = new_level

    # Check the next_level for all keys in this_level
    if isinstance(this_level, dict):
        for key in this_level:
            contruct_arrays_helper(this_level, key)
    elif isinstance(this_level, list):
        for i, key in enumerate(this_level):
            contruct_arrays_helper(this_level, i)

def contruct_arrays(parent_level):
    """Construct arrays from modified dict.
    NOTE: parent_level edited in-place.

        "g": {
            "Array:0": {
                "Array:0": "h",
                "Array:1": "i",
                "Array:2": "j",
                "Array:3": 4,
                "Array:4": 5
            },
            "Array:1": {
                "Array:0": "k",
                "Array:1": "l",
                "Array:2": "m",
                "Array:3": "n",
                "Array:4": "o"
            }
        },
        "p": {
            "Array:0": {
                "id": 6,
                "q": "r"
            },
            "Array:1": {
                "id": 7,
                "s": "t"
            }
        },
        "u": "Array:Empty"

        ------------------------

        "g": [
            [
                "h",
                "i",
                "j",
                4,
                5
            ],
            [
                "k",
                "l",
                "m",
                "n",
                "o"
            ]
        ],
        "p": [
            {
                "id": 6,
                "q": "r"
            },
            {
                "id": 7,
                "s": "t"
            }
        ],
        "u": []

    """

    # Seed the helper method with each of the next_levels
    for key in parent_level:
        contruct_arrays_helper(parent_level, key)

    # Return the dict that was edited in-place
    return parent_level

def flatten_dictionary(this_level, path=None, flat_list=None):
    """Helper method for converting dictionary to lists.
    NOTE: flat_list edited in-place.

        [['a', 1]
        [2, 'b']
        ['c', 'd', 3]
        ['c', 'e', 'f', True]
        ['c', 'g', 'Array:0', 'Array:0', 'h']
        ['c', 'g', 'Array:0', 'Array:1', 'i']
        ['c', 'g', 'Array:0', 'Array:2', 'j']
        ['c', 'g', 'Array:0', 'Array:3', 4]
        ['c', 'g', 'Array:0', 'Array:4', 5]
        ['c', 'g', 'Array:1', 'Array:0', 'k']
        ['c', 'g', 'Array:1', 'Array:1', 'l']
        ['c', 'g', 'Array:1', 'Array:2', 'm']
        ['c', 'g', 'Array:1', 'Array:3', 'n']
        ['c', 'g', 'Array:1', 'Array:4', 'o']
        ['c', 'p', 'Array:0', 'q', 'r']
        ['c', 'p', 'Array:0', 'id', 6]
        ['c', 'p', 'Array:1', 's', 't']
        ['c', 'p', 'Array:1', 'id', 7]]

    """

    # Seed the top level with the appropriate variables
    if not path:
        path = []
        flat_list = []

    try:
        # If the this_level is a dict, recursively flatten all keys
        # in the this_level
        for key in this_level.keys():
            flatten_dictionary(this_level[key], path + [key], flat_list)

    except:
        # Flatten out lists via a special format
        if isinstance(this_level, list):
            # Empty arrays get a special placeholder
            if not this_level:
                flat_list.append(path + ['Array:Empty'])

            # Create the padding count of preceding zeros to allow
            # ASCII sorting
            preceding_zeros = len(this_level) / 10 + 1

            # Iterate over all items in this_level
            for item_id, element in enumerate(this_level):
                # Format keys for Array objects
                formatted_id = str(item_id).zfill(preceding_zeros)

                # Flatten all list items with the keys being composed of
                # their position in array
                item_id = 'Array:%s' % formatted_id
                flatten_dictionary(element, path + [item_id], flat_list)

        # A primitive data type
        else:
            # Append the final this_level item to the path
            flat_list.append(path + [this_level])

    # Return the list that was edited in-place
    return flat_list

def unflatten_dictionary(flat_ordered_dict):
    """Convert an OrderedDict to dictionary."""

    return_dictionary = {}

    for key in flat_ordered_dict:
        # Reset the variables for each key
        next_level = return_dictionary
        path_list = key

        # Trim the path_list as it gets closer to the final key
        while path_list:
            # Find the current level and proper key type
            this_level = next_level
            this_key = string_to_python_type(path_list[0])

            # Create a new depth in return_dictionary, if it doesn't exist
            if not this_key in this_level:
                this_level[this_key] = {}

            # Lead the loop one level down
            next_level = this_level[this_key]
            path_list = path_list[1:]

        # Once the entire path_list has been explored,
        # assign the final value with the proper type
        this_level[this_key] = string_to_python_type(flat_ordered_dict[key])

    contruct_arrays(return_dictionary)
    return return_dictionary, key



class ThreadedSave(threading.Thread):
    """Threaded URL Grab"""

    def __init__(self, db, queue):
        threading.Thread.__init__(self)
        self.db = db
        self.queue = queue

    def run(self):
        while True:
            # grabs dictionary from queue
            contructed_dictionary = self.queue.get()

            cassandra_start_time = time.time()
            try:
                self.db.save(contructed_dictionary)
            except Exception as e:
                logging.error('ThreadedSave Exception (%s): %s', e.errno, e.strerror)

                with open('error.log', 'a') as f:
                    f.write(contructed_dictionary)

            cassandra_end_time = time.time()

            # Calculate and log Cassandra latency
            logging.debug('Cassandra Latency: %s ms',
                          int((cassandra_end_time - cassandra_start_time) * 1000))

            # signals to queue job is done
            self.queue.task_done()



class db(object):
    """Class for easy database manipulation for Cassandra via dictionary."""


    def __init__(self, keyspace, column_family, server_list=['localhost'],
                 write_consistency=ConsistencyLevel.ONE,
                 read_consistency=ConsistencyLevel.ONE,
                 request_size=100, batch_size=6000, thread_count=20):
        """Create the Cassandra connection pool and CF connection."""

        self.keyspace = keyspace
        self.column_family = column_family
        self.pool = pycassa.ConnectionPool(keyspace, server_list=server_list)
        self.cf = pycassa.ColumnFamily(self.pool, self.column_family)

        self.write_consistency = write_consistency
        self.read_consistency = read_consistency

        self.request_size = request_size
        self.batch_size = batch_size

        self.queue = Queue.Queue()

        # Start threads for future multi-threaded asynchronous inserts
        for i in range(thread_count):
            t = ThreadedSave(self, self.queue)
            t.setDaemon(True)
            t.start()

    def save(self, dictionary_payload, write_consistency=None, batch_size=None):
        """Convert and save dictionary into Cassandra."""

        # Use global options, if local options aren't set
        if not write_consistency:
            write_consistency = self.write_consistency
        if not batch_size:
            batch_size = self.batch_size

        for key in dictionary_payload.keys():
            # Flatten dictionary into a list of lists
            flattened_dictionary = flatten_dictionary(dictionary_payload[key])

            try:
                # Create the Pycassa batch mutator that will save in batches
                # In Python >= 2.5, once we exit this block, send() is called
                with self.cf.batch(queue_size=batch_size,
                                   write_consistency_level=write_consistency) as b:

                    for row in flattened_dictionary:
                        # Save object representations of item
                        row = map(repr, row)

                        # Save to Cassandra
                        b.insert(key, {tuple(row[:-1]): row[-1]})

            except InvalidRequestException:
                raise KeyError('Missing Value for %s.%s[%s][%s]. Example: {%s: {%s: <value>}}' %
                               (self.keyspace, self.column_family, key, row[0], key, row[0]))


    def get(self, key, read_consistency=None,
            return_last_row=False, column_start=None, request_size=None):
        """Read and convert Cassandra response into dictionary."""

        # Use global options, if local options aren't set
        if not read_consistency:
            read_consistency = self.read_consistency
        if not request_size:
            request_size = self.request_size

        try:
            if column_start:
                dictionary_out, last_row = unflatten_dictionary(
                        self.cf.get(key,
                        column_count=request_size,
                        read_consistency_level=read_consistency,
                        column_start=column_start))
            else:
                dictionary_out, last_row = unflatten_dictionary(
                        self.cf.get(key,
                        column_count=request_size,
                        read_consistency_level=read_consistency))
        except NotFoundException:
            dictionary_out = {}
            last_row = ()

        # Check if last_row will be returned, for iterator purposes
        if return_last_row:
            return dictionary_out, last_row
        else:
            return dictionary_out

    def delete(self, key, write_consistency=None):
        """Delete dictionary from Cassandra."""

        # Use global options, if local options aren't set
        if not write_consistency:
            write_consistency = self.write_consistency

        if isinstance(key, list):
            for key_i in key:
                self.cf.remove(key_i, write_consistency_level=write_consistency)
        else:
            self.cf.remove(key, write_consistency_level=write_consistency)

    def mt_save(self, dictionary_payload):
        """Save dictionary asynchronously."""

        self.queue.put(dictionary_payload)

    def mt_finish(self):
        """Wait until all pending inserts are performed."""

        self.queue.join()
