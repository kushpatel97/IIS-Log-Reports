from elasticsearch import Elasticsearch
from config import PORT, HOST
import numpy as np

class Logstash():

    def __init__(self):
        """
        When the Logstash object is created the default constructor __init__ is invoked.
        The constructor initializes the elasticsearch object from config.py with the host and port that the elasticsearch is hosted on
        self.header - represents the format of an IIS Server Logs. It was wrapped around a numpy array to increase speed of input (Can be taken out, and won't break anything)
        :param filepaths: A list of filepaths pointed at the *.log file.
        :type filepaths: list
        """
        self.elastic = Elasticsearch([{'host': HOST,'post': PORT}])

        self.header = np.array(['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query','s-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'sc-status', 'sc-substatus', 'sc-win32-status', 'sc-bytes', 'cs-bytes', 'time-taken'])

    def parse(self):
        """
        A function that is supposed to take in the list of filepaths pointed at the logs and creates a list of multiple dictionary.
        The logs are split into a list based on the spaces in a log line and are matched up with the corresponding header fields based on position
        in the list.
        Then zipped into a dictionary, a json format which elasticsearch accepts.
        After the json documents a added to the log_list list, cleanFields is called.
        cleanFields iterates though each json document in the list and removes the fields unnecessary for this application. It also joins the date and time fields together.
        :return: A list of json documents that are ready to insert into elasticsearch
        """
        log_list = []
        filepathList = []
        for file in filepathList:
            open_file = open(file, "r")
            for line in open_file:
                if not line.startswith("#"):
                    fields = np.array(line.split())
                    d = dict(zip(self.header, fields))
                    log_list.append(d)
            open_file.close()
        print('Finished Parsing ...')
        self.cleanFields(log_list)
        print('Finished Cleaning ...')
        return log_list

    def parsev2(self, file):
        """
        A function that is supposed to take in the list of filepaths pointed at the logs and creates a list of multiple dictionary.
        The logs are split into a list based on the spaces in a log line and are matched up with the corresponding header fields based on position in the list.
        Then zipped into a dictionary, a json format which elasticsearch accepts.
        After the json documents a added to the log_list list, cleanFields is called.
        cleanFields iterates though each json document in the list and removes the fields unnecessary for this application. It also joins the date and time fields together.
        :return: A list of json documents that are ready to insert into elasticsearch
        """
        logList=[]
        open_file = open(file, "r")
        for line in open_file:
            if not line.startswith("#"):
                fields = np.array(line.split())
                d = dict(zip(self.header, fields))
                logList.append(d)
        open_file.close()
        print('Finished Parsing ...')
        self.cleanFields(logList)
        print('Finished Cleaning ...')
        return logList

    def cleanFields(self,log_data):
        """
        A method that deletes the fields that weren't needed for this tool
        :param log_data:
        :type log_data: list
        :return:
        """
        for log in log_data:
            log['datetime'] = "{}T{}".format(log['date'],log['time'])
            log['sc-kb'] = float(log['sc-bytes'])/1000
            log['cs-kb'] = float(log['cs-bytes']) / 1000
            del log['time']
            del log['date']
            del log['sc-bytes']
            del log['cs-bytes']
            del log['cs-method']
            del log['s-ip']
            del log['cs-uri-query']
            del log['s-port']
            del log['cs-username']
            del log['cs(User-Agent)']
            del log['sc-status']
            del log['sc-substatus']
            del log['sc-win32-status']


    def createIndex(self, index_name, doc_type):
        """
        Elastich search uses a mapping system to assign data types to json objects so that when they are in Kibana they can easily be aggregatable
        The purpose of this method is to a assign a elasticsearch data type to the parsed fields
        :param index_name: Name of the index we would like to store this information under
        :type index_name: str
        :param doc_type: Document reference for the json objects
        :type doc_type: str
        :return: True if the index was created. False if the index was not created
        """
        settings = {
            "mappings": {
                doc_type: {
                    "properties": {
                        "c-ip": {"type": "ip"},
                        "cs-kb": {"type": "double"},
                        "cs-uri-stem": {"type": "text"},
                        "sc-kb": {"type": "double"},
                        "datetime": {
                            "type": "date",
                            "format": "date_hour_minute_second"
                        },
                        "time-taken": {"type": "integer"}
                    }
                }
            }
        }

        try:
            if not self.elastic.indices.exists(index_name):
                self.elastic.indices.create(index=index_name, body=settings)
                print('Created Index')
                return True
            else:
                print("Index already exists")
                return False
        except Exception as ex:
            print(str(ex))
            return False


    def addToElasticSearch(self, file, index, docType):
        """
        Adds the file to elasticsearch after parsing it and removing the unnecessary fields
        :param file: File path of log
        :type file: str
        :param index_name: Name of the index we would like to store this information under
        :type index_name: str
        :param doc_type: Document reference for the json objects
        :type doc_type: str
        :return:
        """
        logs = self.parsev2(file)
        for line in logs:
            self.elastic.index(index=index, doc_type=docType, id=None, body=line)

    def deleteIndex(self, index):
        """
        Delete an index and everything stored inside
        :param index: The name of the index you would like to delete
        :type index: str
        :return:
        """
        self.elastic.indices.delete(index=index, ignore=[400,404])
        print('Deleted index {}'.format(index))
