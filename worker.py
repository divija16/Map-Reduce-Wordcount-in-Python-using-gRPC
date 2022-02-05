import sys
from concurrent import futures
import glob
from collections import Counter
import grpc
import worker_pb2 as worker
import worker_pb2_grpc as worker_grpc
import time
import threading



class Worker(worker_grpc.WorkerServicer):
    def __init__(self):
        super().__init__()
        self.driver_port = '4000'
        print("Worker init")

    def setDriverPort(self, request, context):
        print("Old driver port", self.driver_port)
        self.driver_port = request.port
        print("New driver port", self.driver_port)
        return worker.status(code=200, msg="OK")

    def die(self, request, context):
        # sys.exit()
        return worker.empty()

    def map(self, request, context):
        # meta data (input)
        file = request.path
        M = request.m
        n= request.mapID
        print("[!] [WORKER] Map operation %i: file: '%s', nbr buckets: %i."%(n, file, M))
        # treatment
        pos = lambda x: ord(x) - 97
        text= ""
        with open(file,"r") as file:
            text = file.read().lower()
        # Tokenizing
        text = ''.join([letter if letter in 'azertyuiopqsdfghjklmwxcvbn' else ' ' for letter in text])
        tokens = text.split()
        # Making tmp lists (buckets)
        tmp = list()
        for i in range(M):
            tmp.append(list())
        # Dispaching the words into their corresponding list
        for token in tokens:
            p = pos(token[0]) % M
            tmp[p].append(token)
        # saving the lists into files
        for idx, tmpFile in enumerate(tmp):
            with open('./tmp/mr-%i-%i'%(n, idx), 'w+') as file:
                file.write('\n'.join(tmpFile))
        return worker.status(code=200, msg="OK")

    def reduce(self, request, context):
        r = request.id
        print("[!] [WORKER] Reduce operation %i."%(r))
        # treatment
        files = glob.glob('./tmp/*-%i'%(r))
        c = Counter([])
        for file in files:
            txt = ''
            with open(file, 'r') as f:
                txt = f.read().split()
            c.update(txt)
        with open('./out/out-%i'%(r), 'w+') as file:
                file.write('\n'.join('%s %s'%(word, count) for word, count in c.items()))
        return worker.status(code=200, msg="OK")

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    worker_grpc.add_WorkerServicer_to_server(Worker(), server)
    port = sys.argv[1]
    server.add_insecure_port("127.0.0.1:%s"%(port))
    server.start()
    print("Worker running on 127.0.0.1:%s"%(port))
    try:
        while True:
            print("Worker is on | nbr threads %i"%(threading.active_count()))
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        server.stop(0)

if __name__ == "__main__":
    server()