# CassandraJsonMapper


CassandraJsonMapper is an Apache Cassandra Python driver developed for direct
functionality to store JSON-style objects mapped into Cassandra composite columns.

This is done via the main methods: `save(key)`, `get(key)`, `delete(key)`.

## History

I needed something small, light-weight, and fast enough on reads and writes for
short bursts of data. I was handling JSON returns from REST API calls that 
I would later do light-weight analytics over. This fit my use case efficiently, 
so I decided to share it.

## Setup

    pip install CassandraJsonMapper


## Initialize Database

Since CassandraJsonMapper uses composite columns heavily to do it's nesting,
the schema for the column family that CassandraJsonMapper will use must look
similar to what is provided. 

**NOTE:**
Keep in mind that the number of composite columns created must be at 
least as deep as the deepest JSON document that will be saved.

```sql
create keyspace json
  with placement_strategy = 'SimpleStrategy'
  and strategy_options = {replication_factor : 1};

use json;

create column family json
  with column_type = 'Standard'
  and comparator = 'CompositeType(
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType,
    org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType)'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'BytesType';
```


## Example Code

```python
import CassandraJsonMapper

db = CassandraJsonMapper.db(keyspace='json', column_family='json')

document = {
    'key2': {
        'test': 'okay'
    }
}
db.save(document)
assert db.get('key2') == document['key2']

db.delete('key2')
assert db.get('key2') == {}
```


## Multi-threaded Example

```python
db.mt_save(document)
db.mt_finish()

assert db.get('key2') == document['key2']
```


## Limitations

In hopes of keeping this client simple, the following is not supported:

* Cannot append to lists.
* Cannot delete portions of a document.
* Reads may not be returned in full, but paging is supported.


## Future Implementations

* Deleting portions of documents.
* Better list support.


## Methods

This client has a few main methods:

```python
def __init__(self, keyspace, column_family, server_list=['localhost'],
             write_consistency=ConsistencyLevel.ONE,
             read_consistency=ConsistencyLevel.ONE,
             request_size=100, batch_size=6000, thread_count=20)
    """Create the Cassandra connection pool and CF connection."""

def save(self, dictionary_payload, write_consistency=None, batch_size=None)
    """Convert and save dictionary into Cassandra."""

def get(self, key, read_consistency=None,
        return_last_row=False, column_start=None, request_size=None)
    """Read and convert Cassandra response into dictionary."""

def delete(self, key, write_consistency=None)
    """Delete dictionary from Cassandra."""

def mt_save(self, dictionary_payload)
    """Save dictionary asynchronously."""

def mt_finish(self)
    """Wait until all pending inserts are performed."""
```
