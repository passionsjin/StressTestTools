"""
ref: https://gist.github.com/gustavosoares/2008272
"""
import os
import sys
import datetime
import time
import random
import traceback
from time import sleep

import pymongo

ACCESS_SAMPLE = '127.0.0.1 - - [29/Apr/2011:18:10:50 -0300] foreman "POST /reports/create?format=yml HTTP/1.1" 200 15 "-" "-" "-" - 0'
ERROR_SAMPLE = '[Tue May 03 14:51:56 2011] [error] [client 0.0.0.0] client denied by server configuration: /mnt/puppet/conf/rack'
HOST = ''
PORT = ''
USER = None
PW = None

class PyMongoManager:
    client = None
    db = None

    def __init__(self, host, port, user=None, password=None):
        self.host = host
        self.port = port
        self.client = pymongo.MongoClient(self.host, self.port, username=user, password=password)
        self.db = self.client['test_db']

    # generic insert function
    def insert(self, k, v):
        self.db.test.insert({"key": k, "value": v})

    # generic Delete all rows function
    def delete_all(self):
        cur = self.db.test.find()
        cur.collection.remove()

    # generic search function
    def search(self, k=None, limit=None):
        cur = None
        if limit:
            if k:
                cur = self.db.test.find({"key": str(k)}).limit(limit)
            else:
                print("query limit: %d" % limit)
                cur = self.db.test.find().limit(limit)
        else:
            if k:
                cur = self.db.test.find({"key": str(k)})
            else:
                cur = self.db.test.find()

        return cur

    def disconnect(self):
        self.client.disconnect()

    # Test Delete Function
    def test_delete_all(self):
        start = time.clock()
        self.delete_all()
        run_time = time.clock()
        return PyMongoManager.get_delta(run_time, start)

    # Test Search function
    def test_search(self, max):
        start = time.clock()
        self.search(random.randint(1, max))
        run_time = time.clock()
        return PyMongoManager.get_delta(run_time, start)

    # Test Insert function
    def test_insert(self, c):
        # create a shuffled key list
        thelist = []
        for i in range(0, c):
            thelist.append(str(i))
        random.shuffle(thelist)
        # start inserting rows (with a random value)
        # print time.clock()
        start = time.clock()
        for i in range(1, c):
            self.insert(thelist[i], random.random())
        run_time = time.clock()
        return PyMongoManager.get_delta(run_time, start)

    @classmethod
    def get_delta(cls, run_time, start):
        # print run_time
        # print start
        return run_time - start


# Calculate average of datetime.datetime values (list)
def average(numbers):
    # total = datetime.datetime.now() - datetime.datetime.now()
    total = 0.0
    for number in numbers:
        total = total + number
    return total / len(numbers)


def clean(limit=10000):
    a = PyMongoManager(HOST, PORT)

    objects = a.search(limit=int(limit))
    # print dir(objects)
    print("total de objetos: %s" % objects.count())
    count = 0
    for obj in objects:
        count = count + 1
    # print obj

    objects.collection.remove()
    print("objetos apagados: %d" % count)
    a.disconnect()


def test():
    a = PyMongoManager(HOST, PORT)
    c = [100, 1000, 10000, 100000, 1000000]  # rows number for each test
    print("######### TEST - n_rows\tinsert\tdelete\tsearch")
    for i in c:
        r_i = a.test_insert(i)  # Insert test
        r_s = list()    # search test
        r_s.append(a.test_search(i))
        r_s.append(a.test_search(i))
        r_s.append(a.test_search(i))
        r_s.append(a.test_search(i))
        avg_s = average(r_s)
        r_d = a.test_delete_all()  # delete test

        # print str(i) + "\t" + str(r_i) + "\t" + str(r_d) + "\t" + str(avg_s)
        print("%d \t %.8f \t %.8f \t %.8f" % (i, r_i, r_d, avg_s))
    # print "\t\t %.6f \t %.6f \t %.6f" % ((i/r_i),(i/r_d),(i/avg_s))
    # print str(i) + "\t" + str(r_i) + "\t" + str(r_d)
    # print "\t\t%.5f\t%.5f\t%.5f" % ((i/secs_i),(i/secs_s),(i/secs_d))
    a.disconnect()


def populate(limit):
    print("populate: %s" % limit)
    if int(limit) < 1000000:
        print("sorry... try harder")
        sys.exit(1)

    a = PyMongoManager(HOST, PORT)

    counter = 0
    line = 0
    counter_time = 500000.0
    start = time.clock()
    for l in range(int(limit)):
        if counter >= counter_time:
            stop = time.clock()
            elapsed = stop - start
            print("*" * 30)
            print("%d - TEMPO: %.8f" % (line, elapsed))
            print("%d - AVG: %.2f inserts/seg" % (line, (float(counter) / float(elapsed))))
            start = stop
            counter = 0

        # insert
        # a.testInsert(1)
        msg = ACCESS_SAMPLE * random.randint(1, 20)
        a.insert(time.clock(), msg)
        counter = counter + 1
        line = line + 1

    a.disconnect()


# 1초에 한번씩 지속적으로 쿼리 날림. 언제 끊기는지 확인.
def connect_test(term=1):
    a = PyMongoManager(HOST, PORT)
    counter = 0
    try:
        while True:
            now_time = datetime.datetime.now()
            a.insert(k=str(now_time), v='Done')
            counter += 1
            print('%d 번째 쿼리 - 시간 : %s' % (counter, now_time))
            sleep(term)
    except:
        traceback.print_exc()
    finally:
        f_a = PyMongoManager(HOST, PORT)
        print('Search Test Insert times')
        print('*' * 50)
        f_a.search(limit=10)
        print('*' * 50)


if __name__ == "__main__":

    if len(sys.argv) == 2:
        action = sys.argv[1]
        if action == "test":
            test()
        elif action == "populate":
            populate(1000000)
            clean()
        elif action == "connect_test":
            connect_test()
            clean()

    elif len(sys.argv) == 3:
        action = sys.argv[1]
        if action == "populate":
            limit = int(sys.argv[2])
            populate(limit)
            clean(limit)
        elif action == "connect_test":
            term = int(sys.argv[2])
            connect_test(term)
            clean()

else:
        print("Uso: %s [connect_test|test|populate|clean] [TOTAL MESSAGES]" % sys.argv[0])
        sys.exit(1)