"""
Copy from (same file name in repository):
https://github.com/Kloke93/database_sync
"""


class DataBase:
    """
    Simple dictionary database
    """
    def __init__(self):
        self.db = {}

    def set_value(self, key, val):
        """
        Sets new key:value to database
        :param key: Key for the database
        :param val: Value of the key
        """
        try:
            self.db[key] = val
            return True
        except KeyError:
            return False

    def get_value(self, key):
        """
        Gets value according to the key of the database
        If key doesn't exist None is returned
        :param key: Key for the database element
        :return: Value from the database if found
        """
        return self.db.get(key)

    def delete_value(self, key):
        """
        Deletes value from database
        :param key: Key for a database value
        :return: Deleted value if existed
        """
        return self.db.pop(key, None)

    def __repr__(self):
        """
        Prints dictionary database
        :return: string description of the database
        """
        return str(self.db)


if __name__ == "__main__":
    dbase = DataBase()
    assert dbase.set_value('1', 'a')
    assert dbase.get_value('1') == 'a'
    assert dbase.get_value('2') is None
    assert dbase.delete_value('2') is None
    assert dbase.delete_value('1') == 'a'
    assert repr(dbase) == '{}'
