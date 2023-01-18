"""
Author: Tomas Dal Farra
Date: 01/19/2023
Description: Defines a class where to handle a dictionary database within a file with win32file
https://github.com/Kloke93/database_sync
"""
from win32file import GENERIC_READ, GENERIC_WRITE, FILE_SHARE_READ, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, CREATE_ALWAYS
from win32file import CreateFile, ReadFile, WriteFile, DeleteFile, GetFileAttributesEx, CloseHandle
from dict_database import DataBase
import logging
import pickle


class FileDataBase(DataBase):
    """
    File handling dictionary database
    """
    def __init__(self, file_name="dbfile.bin"):
        self.file_name = file_name
        super().__init__()
        if not self._non_zero_file():      # creates file with empty dictionary if it doesn't exist
            self.__write_database({})
            logging.debug("New database initialized")
        else:
            self.__read_database()
            logging.debug("Previous database content loaded")

    def _non_zero_file(self) -> bool:
        """
        Checks if a file exists and if it has any content
        :return: if file meets the condition
        """
        try:
            attr = GetFileAttributesEx(self.file_name)
            if attr[4]:
                return True
            return False
        except Exception as err:
            logging.error(f"Error loading the database: {err}")
            return False

    def __read_database(self):
        """ Updates self.db reading database """
        data = b''
        fhandle = 0
        try:
            fhandle = CreateFile(self.file_name,                                        # file name
                                 GENERIC_READ,                                          # read access
                                 FILE_SHARE_READ,                                       # share to read requests
                                 None,                                                  # default security
                                 OPEN_EXISTING,                                         # opens existing files only
                                 FILE_ATTRIBUTE_NORMAL,                                 # normal file
                                 None)                                                  # no attr. template
            read = ReadFile(fhandle, 1024)[1]                # string read from 1024 buffer
            while read:                                      # while read still has data
                data += read
                read = ReadFile(fhandle, 1024)[1]
            self.db = pickle.loads(data)
        finally:
            CloseHandle(fhandle)

    def __write_database(self, data):
        """
        Updates self.db and database file writing something on it
        :param data: data to serialize and write to file
        """
        fhandle = 0                                                         # just making fhandle exist
        try:
            fhandle = CreateFile(self.file_name,                            # file name
                                 GENERIC_WRITE,                             # read access
                                 0,                                         # do not share
                                 None,                                      # default security
                                 CREATE_ALWAYS,                             # always creates a new file
                                 FILE_ATTRIBUTE_NORMAL,                     # normal file
                                 None)                                      # no attr. template
            s_data = pickle.dumps(data)
            WriteFile(fhandle, s_data)
        finally:
            CloseHandle(fhandle)

    def set_value(self, key, val) -> bool:
        """
        Sets new key:value to database in file
        :param key: Key for the database
        :param val: Value of the key
        :return: If the operation was successful
        """
        try:
            self.__read_database()
            is_set = super().set_value(key, val)
            self.__write_database(self.db)                    # writes to file serialization of self.db
            return is_set
        except Exception as err:
            logging.error(f"There was a problem to set value: {err}")
            raise err

    def get_value(self, key):
        """
        Gets value according to the key of the database in file
        If key doesn't exist None is returned
        :param key: Key for the database element
        :return: Value from the database if found
        """
        try:
            self.__read_database()
            return super().get_value(key)
        except Exception as err:
            logging.error(f"There was a problem to get value: {err}")
            raise err

    def delete_value(self, key):
        """
        Deletes value from database in file
        :param key: Key for a database value
        :return: Deleted value if existed
        """
        try:
            self.__read_database()
            val = super().delete_value(key)
            self.__write_database(self.db)                    # writes to file serialization of self.db
            return val
        except Exception as err:
            logging.error(f"There was a problem to delete value: {err}")
            raise err

    def get_name(self) -> str:
        """
        Gets file name
        :return: file name
        """
        return self.file_name

    def __repr__(self):
        """
        Prints file name and then file dictionary
        :return: string description of the database
        """
        try:
            self.__read_database()
        except Exception as err:
            logging.error(f"There was a problem trying to print database: {err}")
            raise err
        return f"{self.file_name}: " + super().__repr__()


if __name__ == "__main__":
    database = FileDataBase('testfile.bin')
    try:
        assert database.set_value('1', '2')
        assert database.set_value(1, '3')
        assert database.get_value(1) == '3'
        assert database.delete_value('1') == '2'
        assert database.get_value('1') is None
        assert database.delete_value('1') is None
        assert repr(database) == database.get_name() + ": {1: '3'}"
    finally:
        DeleteFile(database.get_name())
    # logging configuration just when running
    # log_file = "file_database.log"                                                   # file to save the log
    # log_level = logging.DEBUG                                                        # set the minimum logger level
    # log_format = "[%(filename)s] - %(asctime)s - %(levelname)s - %(message)s"        # logging format
    # logging.basicConfig(filename=log_file, level=log_level, format=log_format)
