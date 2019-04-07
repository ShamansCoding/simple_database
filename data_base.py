#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Simple RAM database implementation with nested transactions."""

from __future__ import print_function

from builtins import input, dict


HELP = """
Enter method name, key or value(or both if required) separated by whitespace
to interact with the storage.

Methods:
    SET    - Store key and it's value. (Example: SET A 10)
    GET    - Get value by key. (Example: GET A)
    UNSET  - Remove given key. (Example: UNSET A)
    COUNTS - Count occurance of value in storage. (Example: COUNTS 10)
    FIND   - Find all keys for value. (Example: FIND 10)
    END    - Exit database.

Transactions:
    BEGIN    - Start new transaction.
    ROLLBACK - Rollback curret transaction.
    COMMIT   - Commit all changes for all transactions.
"""


class DataBase(object):
    """Dictionary storage with DML commands."""

    def __init__(self):
        """Initialiaze storage."""
        self._storage = dict()
        self._rolling_back = False
        self._transaction_number = 0
        self._rollback_cache = dict()

    def SET(self, key, value):
        """Set new key-value pair.

        :param str key:
        :param str value:
        """
        if self._transaction_number and not self._rolling_back:

            if key not in self._storage:
                self._rollback_cache[self._transaction_number].append(
                    ('UNSET', key)
                )
            else:
                self._rollback_cache[self._transaction_number].append(
                    ('SET', key, self._storage[key])
                )

        self._storage[key] = value

    def GET(self, key):
        """Extract value by given key.

        If not found prints NULL.

        :param str key:
        """
        print(self._storage.get(key, 'NULL'))

    def UNSET(self, key):
        """Remove given key.

        If not found does nothing.

        :param str key:
        """
        if key in self._storage:
            if self._transaction_number and not self._rolling_back:
                self._rollback_cache[self._transaction_number].append(
                    ('SET', key, self._storage[key])
                )
            del self._storage[key]
        else:
            pass

    def COUNTS(self, value):
        """Count occurance of value in storage.

        :param str value:
        """
        print(len([occurance for occurance in self._storage.values()
                   if value == occurance]))

    def FIND(self, value):
        """Find all keys for value.

        :param str value:
        """
        print(' '.join([key for key, occurance in self._storage.items()
              if value == occurance]))

    def BEGIN(self):
        """Start transaction."""
        self._transaction_number += 1
        self._rollback_cache[self._transaction_number] = []

    def ROLLBACK(self):
        """Rollback current transaction."""
        if not self._transaction_number:
            pass

        self._rolling_back = True

        for cache in reversed(self._rollback_cache[self._transaction_number]):
            method = cache[0]
            args = cache[1:]
            getattr(self, method)(*args)

        del self._rollback_cache[self._transaction_number]

        if self._transaction_number != 0:
            self._transaction_number -= 1

        self._rolling_back = False

    def COMMIT(self):
        """Clear rollback_cache and set transaction number to 0."""
        self._rollback_cache = dict()
        self._transaction_number = 0


def call_method(instance, method_and_args):
    """Call method with given args.

    :param instance: class instance.
    :param list[str] method_and_args:

    :rtype: bool
    """
    try:
        method = method_and_args[0]
        args = method_and_args[1:]
        getattr(instance, method)(*args)
    except AttributeError:
        print(' -> Invalid method name. To get methods names type HELP.')
        return False
    except TypeError:
        print(' -> Invalid number of argumets for', method)
        return False

    return True


def main():
    """Start database."""
    print('Welcome to simple database! Type HELP for help.')

    database = DataBase()

    while True:
        user_input = input(' -> ').split(' ')

        if user_input[0] == 'HELP':
            print(HELP)
            continue
        elif user_input[0] == 'END':
            print('Exiting database...')
            break

        call_method(database, user_input)


if __name__ == '__main__':
    main()
