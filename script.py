from time import sleep,time
from logstash import Logstash
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


logstash = Logstash()



class MyHandler(PatternMatchingEventHandler):


    def __init__(self, *args, **kwargs):
        super(MyHandler, self).__init__(*args, **kwargs)
        self.last_created = None

    def on_created(self, event):
        path = event.src_path
        if path != self.last_created:
            url = event.src_path.replace('\\','/')

            if logstash.createIndex(index_name='v2', doc_type='iis_logs') == True:
                logstash.addToElasticSearch(file=url,index='v2', docType='iis_logs')
            else:
                logstash.addToElasticSearch(file=url,index='v2', docType='iis_logs')

            self.last_created = path
            print("Last created", self.last_created)

    # def on_deleted(self, event):
    #     path = event.src_path
    #     print(path)
    #     if path == self.last_created:
    #         self.last_created = None


if __name__ == '__main__':
    event_handler = MyHandler(patterns=["*.log"])
    observer = Observer()
    observer.schedule(event_handler, path='C:\inetpub\logs\TestLogFiles', recursive=True)
    observer.start()

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
