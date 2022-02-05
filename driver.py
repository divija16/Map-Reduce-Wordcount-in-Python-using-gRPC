import sys
from concurrent import futures
import threading
import glob
from worker import Worker
import grpc

import driver_pb2 as driver
import driver_pb2_grpc as driver_grpc

import worker_pb2 as worker
import worker_pb2_grpc as worker_grpc

import time
import threading


class Driver(driver_grpc.DriverServicer):
    def __init__(self):
        super().__init__()
        self.workers = dict()
        self.treatFiles = dict()
        print("Driver init")

        
    def launchDriver(self, request, context):
        def getWorker():
            for k, v in self.workers.items():
                if v[0] == 0:
                    return k
            return False

        def mapIt(key, file, mid, nbrReduce):
            print(f"[*] [DRIVER] [MAP] Map operation '{mid}' on the file '{file}' is sent to worker '{key}'...")
            self.workers[key][0] = 1
            r = worker.mapInput(path=file, mapID=mid, m=nbrReduce)
            r = self.workers[key][1].map(r)
            self.workers[key][0] = 0
            print(f"[!] [DRIVER] [MAP] Map operation '{mid}' on the file '{file}' terminated with code: '{r.code}' and message: {r.msg}.")
            return r

        def reduceIt(key, rid):
            print(f"[*] [DRIVER] [REDUCE] Reduce operation '{rid}' is sent to worker '{key}'...")
            self.workers[key][0] = 1
            r = worker.rid(id=rid)
            r = self.workers[key][1].reduce(r)
            self.workers[key][0] = 0
            print(f"[!] [DRIVER] [REDUCE] Reduce operation '{rid}' terminated with code: '{r.code}' and message: {r.msg}.")
            return r

        print("[!] [DRIVER] [CONFIG] Driver is launching")
        # Detecting the input files
        files = glob.glob(request.dirPath + "/*.txt")
        print("[!] [DRIVER] [CONFIG] Detected %i files in the directory '%s'."%(len(files), request.dirPath))
        M = request.m
        # Saving the workers ports
        ports = [int(port) for port in  request.ports.split('|')]
        print("[!] [DRIVER] [CONFIG] Requested %i workers with the following ports: [%s]."%(len(ports), ' '.join(str(s) for s in ports)))
        for file in files:
            self.treatFiles[file] = 0
        # connect to the workers
        for port in ports:
            channel = grpc.insecure_channel(f'localhost:{port}')
            print(f"[*] [DRIVER] [CONFIG] Connecting to worker with port: {port}...")
            try:
                grpc.channel_ready_future(channel).result(timeout=10)
            except grpc.FutureTimeoutError:
                sys.exit(f"[-] [ERROR] Could not connect to worker'{port}'.")
            print(f"[!] [DRIVER] [CONFIG] Connection with worker '{port}' established.")
            self.workers[port] = [0, worker_grpc.WorkerStub(channel)]
            self.workers[port][1].setDriverPort(worker.driverPort(port=int(sys.argv[1])))
            # Printing infos
        print("[!] [DRIVER] [CONFIG]  Registered: %i map operations and %i reduce operations"%(len(files), M))
        print()
        print(f"[!] [DRIVER] [MAP] Starting the map phase.")
        start_time = time.time()
        # dispath map tasks
        with futures.ThreadPoolExecutor() as executor:
            for idx, file in enumerate(files):
                print(f"[*] [DRIVER] [MAP] Launching map operation '{idx}' on the file '{file}'...")
                tmpWorker = getWorker()
                while tmpWorker == False:
                    print(f"[!] [DRIVER] [MAP] Map operation '{idx}' is paused due to all workers being occupied.")
                    time.sleep(5)
                    tmpWorker = getWorker()
                print(f"[!] [DRIVER] [MAP] Launching map operation '{idx}' on the file '{file}' started.")
                executor.submit(mapIt, key=tmpWorker, file=file, mid=idx, nbrReduce=M)
        
        print(f"[!] [DRIVER] [MAP] Map phase terminated in '{time.time()-start_time}' second(s).")
        print()
        print(f"[!] [DRIVER] [REDUCE] Starting the reduce phase.")
        start_time = time.time()
        # dispath reduce tasks
        with futures.ThreadPoolExecutor() as executor:
            for idx in range(M):
                print(f"[*] [DRIVER] [REDUCE] Finding a worker for reduce operation '{idx}'...")
                tmpWorker = getWorker()
                while tmpWorker == False:
                    print(f"[!] [DRIVER] [REDUCE] Reduce operation '{idx}' is paused due to all workers being occupied.")
                    time.sleep(5)
                    tmpWorker = getWorker()
                    
                #print(f"[!] [DRIVER] [REDUCE] Launching reduce operation '{idx}'.")
                executor.submit(reduceIt, key=tmpWorker, rid=idx)
        print(f"[!] [DRIVER] [REDUCE] Reduce phase terminated in '{time.time()-start_time}' second(s).")
        
        for port, (stat, stub) in self.workers.items():
            stub.die(worker.empty())
        return driver.status(code=200, msg="OK")

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    driver_grpc.add_DriverServicer_to_server(Driver(), server)
    port = sys.argv[1]
    server.add_insecure_port("127.0.0.1:%s"%(port))
    server.start()
    print("Driver running on 127.0.0.1:%s"%(port))
    try:
        while True:
            print("Driver is on | nbr threads %i"%(threading.active_count()))
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        server.stop(0)

if __name__ == "__main__":
    server()