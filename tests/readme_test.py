#!/usr/bin/env python

def example():
## Example Code

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

## Multi-threaded Example

    db.mt_save(document)
    db.mt_finish()

    assert db.get('key2') == document['key2']

if __name__ == "__main__":
    example()
