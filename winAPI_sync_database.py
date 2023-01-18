"""
Author:
Date:
Description:
https://github.com/Kloke93/database_sync
"""
from win32event import CreateMutex, CreateSemaphore, ReleaseMutex, ReleaseSemaphore
from win32event import CreateEvent, SetEvent, ResetEvent
from win32event import WaitForSingleObject, WAIT_ABANDONED, WAIT_OBJECT_0, WAIT_TIMEOUT
from winAPI_file_database import FileDataBase
import logging


class SyncDataBase(FileDataBase):
    """
    Simple database thread/process synchronized
    """
    # All the not_reading event part was commented out because it didn't work.
    READERS_BOUND = 10
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)              # set the minimum logger level

    def __init__(self, mode, file_name="dbfile.bin"):
        """
        Initializer for synchronized database class
        :param mode: Takes a flag 1 or 0 where this means threading or multiprocessing correspondingly
        :param file_name: Name of file for the database
        """
        # Event to check if there are writers
        self.not_writing = CreateEvent(None,                            # default security attributes
                                       True,                            # manual-reset event
                                       True,                            # initial state is signaled
                                       "not_writing")                   # event name

        # Mutex for writers
        self.to_write_lock = CreateMutex(None,                          # default security attributes
                                         False,                         # initially not owned
                                         "to_write_lock")               # mutex name

        # Semaphore to limit readers and for one writer at a time
        self.semaphore = CreateSemaphore(None,                          # default security attributes
                                         SyncDataBase.READERS_BOUND,    # initial count
                                         SyncDataBase.READERS_BOUND,    # maximum count
                                         "limit_semaphore")             # unnamed semaphore
        # logging format
        if mode or True:
            formatter = logging.Formatter("[%(filename)s][%(threadName)s][%(asctime)s] %(message)s")
        else:
            formatter = logging.Formatter("[%(filename)s][%(processName)s][%(asctime)s] %(message)s")
        # logger configuration
        file_handler = logging.FileHandler("file_database.log")         # file to save the log
        file_handler.setFormatter(formatter)
        SyncDataBase.logger.addHandler(file_handler)
        SyncDataBase.logger.info(f"Start in mode {mode}")
        # creating synchronized instance
        self.__lock_write()
        super().__init__(file_name)
        self.__release_write()

    @staticmethod
    def __check_wait(w):
        """ Checks if WaitForSingleObject was successfully executed """
        if w == WAIT_OBJECT_0:
            return None
        elif w == WAIT_ABANDONED:
            SyncDataBase.logger.error("Wait was abandoned")
            raise Exception
        elif w == WAIT_TIMEOUT:
            SyncDataBase.logger.error("Timeout was exceeded")
            raise Exception

    def __lock_write(self):
        """ Manages locking for writing functions """
        w = WaitForSingleObject(self.to_write_lock,                 # locks for other writers
                                100000)                             # waits up to 100 seconds
        self.__check_wait(w)
        ResetEvent(self.not_writing)                                # There is someone writing (has priority)
        for i in range(SyncDataBase.READERS_BOUND):
            w = WaitForSingleObject(self.semaphore,                 # gets the whole semaphore buffer
                                    100000)                         # waits up to 100 seconds
            self.__check_wait(w)

    def __release_write(self):
        """ Manages releasing locking for writing functions """
        ReleaseSemaphore(self.semaphore,                            # releases semaphore
                         SyncDataBase.READERS_BOUND)                # increases count up to his maximum count
        SetEvent(self.not_writing)                                  # Indicates no one is writing
        ReleaseMutex(self.to_write_lock)                            # releases lock for other writers

    def __lock_read(self):
        """ Manages locking for reading functions """
        w = WaitForSingleObject(self.not_writing,                   # waits until no one is writing
                                100000)                             # waits up to 100 seconds
        self.__check_wait(w)
        w = WaitForSingleObject(self.semaphore,                     # reduces semaphore counter by 1 and blocks if = 0
                                100000)                             # waits up to 100 seconds
        self.__check_wait(w)

    def __release_read(self):
        """ Manages releasing locking for reading functions """
        ReleaseSemaphore(self.semaphore,                            # increases semaphore count by one
                         1)

    def set_value(self, key, val) -> bool:
        """
        Sets new key:value to database in file synchronized
        :param key: Key for the database
        :param val: Value of the key
        :return: If the operation was successful
        """
        self.__lock_write()
        try:
            is_set = super().set_value(key, val)
        except Exception as err:
            SyncDataBase.logger.error(f"Error setting key<{key}> to value<{val}>: {err}")
            raise err
        finally:
            self.__release_write()
        return is_set

    def get_value(self, key):
        """
        Gets value according to the key of the database in file synchronized
        If key doesn't exist None is returned
        :param key: Key for the database element
        :return: Value from the database if found
        """
        self.__lock_read()
        try:
            val = super().get_value(key)
        except Exception as err:
            SyncDataBase.logger.error(f"Error getting key<{key}>: {err}")
            raise err
        finally:
            self.__release_read()
        return val

    def delete_value(self, key):
        """
        Deletes value from database in file synchronized
        :param key: Key for a database value
        :return: Deleted value if existed
        """
        self.__lock_write()
        try:
            deleted = super().delete_value(key)
        except Exception as err:
            SyncDataBase.logger.error(f"Error deleting key<{key}>: {err}")
            raise err
        finally:
            self.__release_write()
        return deleted

    def _set_value_testing(self, key) -> bool:
        """ Special set_value modification to change previous value of key in dictionary by one"""
        self.__lock_write()
        try:
            val = super().get_value(key)
            is_set = super().set_value(key, val + 1)
        except Exception as err:
            SyncDataBase.logger.error(f"Error setting (test) key<{key}>: {err}")
            raise err
        finally:
            self.__release_write()
        return is_set
