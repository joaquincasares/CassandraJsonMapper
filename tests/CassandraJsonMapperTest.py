#!/usr/bin/env python

import CassandraJsonMapper
import pycassa

try: import simplejson as json
except: import json




def json_format(dict_input):
    """Returns a dict in proper json output. For debugging purposes."""

    return json.dumps(dict_input, sort_keys=True, indent=4)

def test(host='localhost', keyspace_provided=None):
    from pycassa.types import BytesType, CompositeType
    from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY

    # Create a fake keyspace, if not provided
    if not keyspace_provided:
        import random
        keyspace = '%s_%s' % ('json', random.randrange(1000000, 100000000))
    else:
        keyspace = keyspace_provided

    # Connect to cluster and create keyspace
    system_manager = SystemManager(host)
    system_manager.create_keyspace(keyspace, SIMPLE_STRATEGY,
                                   {'replication_factor': '1'})
    try:
        # Create CF with many CompositeFields
        comparator = CompositeType(BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType(),
                                   BytesType(), BytesType(), BytesType())
        system_manager.create_column_family(keyspace, 'json',
                                            comparator_type=comparator)



        # Connect to the KS/CF and save samples
        db = CassandraJsonMapper.db(keyspace, 'json')

        tests = 0
        # ----------------------------------------------------------------------
        # Test a complicated structure
        sample_1 = {
            'key1': {
                'a': 1,
                2: 'b',
                'c': {
                    'd': 3,
                    'e': {
                        'f': True
                    },
                    'g': [
                        [
                            'h',
                            'i',
                            'j',
                            4,
                            5
                        ], [
                            'k',
                            'l',
                            'm',
                            'n',
                            'o'
                        ],
                    ],
                    'p': [
                        {
                            'id': 6,
                            'q': 'r'
                        }, {
                            'id': 7,
                            's': 't'
                        }
                    ],
                    'u': [],
                    'v': None
                }
            }
        }
        db.save(sample_1)
        if db.get('key1') != sample_1['key1']:
            raise AssertionError('What was saved is not being equally returned.')
        tests += 1

        # ----------------------------------------------------------------------
        # Test improper format missing values
        sample_2 = {
            'key1': 1
        }
        try:
            db.save(sample_2)
            raise AssertionError('sample_2 should have thrown a KeyError.')
        except KeyError:
            pass
        tests += 1

        # ----------------------------------------------------------------------
        # Test saving multiple keys
        sample_3 = {
            'key2': {2:2},
            'key3': {3:3},
            'key4': {4:4}
        }
        db.save(sample_3)
        if db.get('key2') != {2:2} or db.get('key3') != {3:3} or \
           db.get('key4') != {4:4}:

            raise AssertionError('Not all keys in a json_payload were saved.')
        tests += 1

        # ----------------------------------------------------------------------
        # Test delete
        db.delete('key1')
        if db.get('key1'):
            raise AssertionError('Not all keys in were delted.')
        tests += 1

        # ----------------------------------------------------------------------
        # Test deletes
        db.delete(['key2', 'key3', 'key4'])
        if db.get('key2') or db.get('key3') or db.get('key4'):
            raise AssertionError('Not all keys in were delted.')
        tests += 1

        # ----------------------------------------------------------------------
        # Test reading from fake keys
        if db.get('fake_key') != {}:
            raise AssertionError('A get(fake_key) should return {}.')
        tests += 1

        # ----------------------------------------------------------------------
        # Test multi-threaded save
        db.mt_save(sample_1)
        db.mt_finish()
        if db.get('key1') != sample_1['key1']:
            raise AssertionError('What was saved is not being equally returned.')
        tests += 1

        # ----------------------------------------------------------------------

        # print json_format(db.get('key1'))
        print '{0}/{0} tests passed!'.format(tests)

    except:
        raise
    finally:
        # For debugging purposes
        # raw_input('Press ENTER to continue...')

        if not keyspace_provided:
            # Delete the temporary KS's
            system_manager.drop_keyspace(keyspace)
            system_manager.close()



if __name__ == "__main__":
    test()
