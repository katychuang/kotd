import time
import csv

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

import settings

class kafkaProducer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

    def createTestData(self, a, b):
        self.producer.send_messages("test_a", "{},{}".format(a, b))
        print("Sending '{}, {}'".format(a, b))

    def createImageData(self, my_id, username, link, location, created_time, likes_count, comments_time, img_standard):
        stringRow = "%s,%s,%s,%s,%s,%s,%s,%s" % (my_id, username, link, location, created_time, likes_count, comments_time, img_standard)
        self.producer.send_messages("kotd-0914b", stringRow)
        print(stringRow)

    def createShoeData(self, title, location, price):
        stringRow = "%s,%s,%s" % (title, location, price)
        self.producer.send_messages("kotd-e0917a", stringRow)
        # print(stringRow)

    def readAndStreamTest(self, data_path):
        with open(data_path, "rU") as csvfile:
            print("starting test")
            filereader = csv.reader(csvfile, delimiter=',')
            for row in filereader:
                a = row[0]
                b = row[1]
                self.createTestData(a, b)
                time.sleep(1) 

    def readAndStreamWorn(self, data_path):
        with open(data_path, "rU") as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='|')
            fields = filereader.next()
            # skip header
            for row in filereader: 
                # csvcut -n try.csv
                location = row[3]  # row[32]
                created_time = row[6]  # row[42]
                link = row[7]  # row[43]
                likes_count = row[8]  # row[44]
                img_standard = row[9]  # row[63]
                username = row[14]  # row[75]
                comments_time = row[4]  # row[89]
                my_id = row[13]  # row[74]
                self.createImageData(my_id, username, link, location, created_time, likes_count, comments_time, img_standard)
                time.sleep(1)

    def readAndStreamSales(self, data_path):
        with open(data_path, "rU") as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in filereader:
                title = row[0]
                location = row[1]
                price = row[2]
                self.createShoeData(title, location, price)
                time.sleep(1)

url = settings.master_node
myProducer = kafkaProducer(url)
# myProducer.readAndStreamWorn("sample1.csv")
# myProducer.readAndStreamSales("ebaylist.csv")
myProducer.readAndStreamTest("test.csv")
