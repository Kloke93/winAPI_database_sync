"""
Author: Tomas Dal Farra
Date: 01/19/2023
Description: Test file for threading and multiprocessing synchronized with win api. Modified from:
https://github.com/Kloke93/database_sync
"""
from winAPI_sync_database import SyncDataBase
from random import randint
import multiprocessing
import threading
import unittest
import pickle
import os


class TestThreadDB(unittest.TestCase):
    """ Class to test synchronized database in threading mode """
    test_fname = "testfile.bin"
    reps = 5000

    def set_value(self, key, val):
        """ Test set_value method """
        for _ in range(TestThreadDB.reps):
            self.assertTrue(self.sync_db.set_value(key, val))

    def get_value(self, key):
        """ Test get_value method """
        for _ in range(TestThreadDB.reps):
            self.assertEqual(self.sync_db.get_value(key), key * 100)

    def set_value_special(self, key):
        """ Test set_value method based on a previous value """
        for _ in range(TestThreadDB.reps):
            self.assertTrue(self.sync_db._set_value_testing(key))

    def get_value_exists(self, key):
        """ Test get_value method just that returns some int """
        for _ in range(TestThreadDB.reps):
            self.assertIsInstance(self.sync_db.get_value(key), int)

    @staticmethod
    def get_database_dict():
        """
        Gets the database dictionary
        :return: Dictionary from file
        """
        with open(TestThreadDB.test_fname, "rb") as f:
            db = pickle.load(f)
        return db

    def setUp(self):
        """
        sets up the testing file with a specific dictionary and creates an instance of the database
        """
        self.test_dict = {n: n * 100 for n in range(1, 51)}
        with open(TestThreadDB.test_fname, "wb") as f:
            pickle.dump(self.test_dict, f)
        self.sync_db = SyncDataBase(1, TestThreadDB.test_fname)

    def test_write_simple(self):
        """ Tests writing without competition """
        thread = threading.Thread(target=self.set_value, name="thread_ws", args=(40, 4002))
        thread.start()
        thread.join()
        self.test_dict[40] = 4002
        self.assertEqual(self.get_database_dict(), self.test_dict)

    def test_read_simple(self):
        """ Tests reading without competition """
        thread = threading.Thread(target=self.get_value, name="thread_ws", args=(40,))
        thread.start()
        thread.join()
        self.assertEqual(self.get_database_dict(), self.test_dict)

    def test_general(self):
        """ Tests of 12 threads trying to read and write (9 read and 3 that do both) changes correctly """
        randbers = [randint(21, 50)] * 3          # 1 random numbers between 21 and 50. 3 times in list
        threads = []
        # append 9 reading threads between 3 random numbers
        for i in range(1, 10):
            t = threading.Thread(target=self.get_value_exists, name=f"thread_{i}", args=(randbers[0],))
            threads.append(t)
        # append 3 writing threads that increase number of value by 1 each iteration
        for i in range(10, 13):
            t = threading.Thread(target=self.set_value_special, name=f"thread_{i}", args=(randbers[0],))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for i in randbers:
            self.test_dict[i] += TestThreadDB.reps
        self.assertEqual(self.get_database_dict(), self.test_dict)

    def tearDown(self):
        """
        Deletes the testing file
        """
        os.remove("testfile.bin")


class TestProcessDB(unittest.TestCase):
    """ Class to test synchronized database in multiprocessing mode """
    test_fname = "testfile.bin"
    reps = 5000

    # In processes methods does not execute unittest asserts within themselves
    @staticmethod
    def set_value(key, val):
        """ Test set_value method """
        sync_db = SyncDataBase(0, TestProcessDB.test_fname)
        for _ in range(TestProcessDB.reps):
            sync_db.set_value(key, val)

    @staticmethod
    def get_value(key):
        """ Test get_value method """
        sync_db = SyncDataBase(0, TestProcessDB.test_fname)
        for _ in range(TestProcessDB.reps):
            sync_db.get_value(key)

    @staticmethod
    def set_value_special(key):
        """ Test set_value method based on a previous value """
        sync_db = SyncDataBase(0, TestProcessDB.test_fname)
        for _ in range(TestProcessDB.reps):
            sync_db._set_value_testing(key)

    @staticmethod
    def get_value_exists(key):
        """ Test get_value method just that returns some int """
        sync_db = SyncDataBase(0, TestProcessDB.test_fname)
        for _ in range(TestProcessDB.reps):
            sync_db.get_value(key)

    @staticmethod
    def get_database_dict():
        """
        Gets the database dictionary
        :return: Dictionary from file
        """
        with open(TestProcessDB.test_fname, "rb") as f:
            db = pickle.load(f)
        return db

    def setUp(self):
        """
        sets up the testing file with a specific dictionary and creates an instance of the database
        """
        self.test_dict = {n: n * 100 for n in range(1, 51)}
        with open(TestProcessDB.test_fname, "wb") as f:
            pickle.dump(self.test_dict, f)

    def test_write_simple(self):
        """ Tests writing without competition """
        proc = multiprocessing.Process(target=self.set_value, name="proc_ws", args=(40, 4002))
        proc.start()
        proc.join()
        self.test_dict[40] = 4002
        self.assertEqual(self.get_database_dict(), self.test_dict)

    def test_read_simple(self):
        """ Tests reading without competition """
        proc = multiprocessing.Process(target=self.get_value, name="proc_ws", args=(40,))
        proc.start()
        proc.join()
        self.assertEqual(self.get_database_dict(), self.test_dict)

    def test_general(self):
        """ Tests of 12 processes trying to read and write (9 read and 3 that do both) changes correctly """
        randbers = [randint(21, 50)] * 3  # 1 random numbers between 21 and 50. 3 times in list
        procs = []
        # append 9 reading processes between 3 random numbers
        for i in range(1, 10):
            p = multiprocessing.Process(target=self.get_value_exists, name=f"proc_{i}", args=(randbers[0],))
            procs.append(p)
        # append 3 writing processes that increase number of value by 1 each iteration
        for i in range(10, 13):
            p = multiprocessing.Process(target=self.set_value_special, name=f"proc_{i}", args=(randbers[0],))
            procs.append(p)
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        for i in randbers:
            self.test_dict[i] += TestProcessDB.reps
        self.assertEqual(self.get_database_dict(), self.test_dict)

    def tearDown(self):
        """
        Deletes the testing file
        """
        os.remove("testfile.bin")

    # https://stackoverflow.com/questions/25646382/python-3-4-multiprocessing-does-not-work-with-unittest
    # Dano answer to understand
    # __getstate__ and __setstate can be ignored
    def __getstate__(self):
        self_dict = self.__dict__.copy()
        del self_dict['_outcome']
        return self_dict

    def __setstate(self, state):
        self.__dict__.update(self_dict)


if __name__ == "__main__":
    unittest.main()