#!/usr/bin/env python

#select LEFT(data, 50),email AS LongField_First20chars from accounts;

from multiprocessing import Pool
import random
import string
import time
import hashlib
import psycopg2
 
def compute_cluster(c):
    conn = psycopg2.connect(dbname="db", user="postgres", password="postgress", host="127.0.0.1")
    cursor = conn.cursor()

    d=random_bytes(100)#len name file
    e=random_email(10)
    try:
      cursor.execute("INSERT INTO accounts (data, email) VALUES ('{}', '{}')".format(d,e))
      conn.commit()
    except psycopg2.IntegrityError as e:
        conn.rollback()
        print("dupl")
    #insert into accounts (data,email) values (01,'CentOS'); 
    cursor.close()
    conn.close()

def random_email(n):
    random.seed(random.randint(1,100000000))
    name = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=n))
    #print("Name = ",name)
    #start_time = time.time()
    #data = bytearray(random.randint(0, 255) for i in range(20*1024)) #size file 40kb
    #print("--- %s seconds BYTES ---" % (time.time() - start_time))
    #print(len(data.hex()))
    #start_time = time.time()
    data = ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))
    #print("--- %s seconds STRING---" % (time.time() - start_time))
    #print(len(data))
    
    #fp = open('/tmp/123/files/'+name+'@mail.ru', 'w')
    #fp.write(data.hex())
    #fp.close
    #print(data.hex())
    return name+'@mail.ru';
    
def random_bytes(n):
    random.seed(random.randint(1,1000000000))
    name = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=n))
    #print("Name = ",name)
    #start_time = time.time()
    #fp = open('/tmp/123/files/'+name+'.txt', 'w')
    
    #start_time = time.time()
    data = bytearray(random.randint(0, 255) for i in range(6*1024)) #size file 40kb
        #print("--- %s 11111 ---" % (time.time() - start_time))
    #print(len(data.hex()))
        #data = bytearray(random.randint(0, 255) for i in range(5)) #size file 40kb
        #start_time = time.time()
    #data = ''.join(random.choices(string.ascii_uppercase + string.digits, k=1024))
    #print("--- %s seconds STRING---" % (time.time() - start_time))
    #print(len(data))
    
    #fp = open('/tmp/123/files/'+name+'.txt', 'w')
    #fp.write(data.hex())
    #print("--- %s 2222---" % (time.time() - start_time))
    #
    #fp.close
    return data.hex()
    #print(data.hex())
if __name__=="__main__":
   pool = Pool(8) # run 10 task at most in parallel
   pool.map(compute_cluster, range(30000))

#random_bytes(random.randint(1,10000))
