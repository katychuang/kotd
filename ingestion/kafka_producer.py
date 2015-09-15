import time
import csv

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

import settings


class kafkaProducer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = SimpleProducer(self.client)

    def createImageData(self, my_id, username, link, location, created_time, likes_count, comments_time, img_standard):
        stringRow = "%s,%s,%s,%s,%s,%s,%s,%s" % (my_id, username, link, location, created_time, likes_count, comments_time, img_standard)
        self.producer.send_messages("kotd-0914b", stringRow)
        print(stringRow)

    def readAndStream(self):
        data_path = "data/sample1.csv"
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

url = settings.master_node
igProducer = kafkaProducer(url)
igProducer.readAndStream()
