import docker
import time
from docker.models.containers import ContainerCollection,Container
from queue import Empty, Queue
from subprocess import PIPE, Popen
from threading import Event, Thread
from time import sleep, time
from typing import Any, cast
from datetime import datetime
from typing import Literal, NotRequired, TypedDict
import hashlib
import json
## Create a Docker client

class ContainerStatsRef(TypedDict):
    """A container stats ref object compare between current and past stats.

    Attributes
    ----------
    key
        The reference key of a stat rotation
    last
        When the last stat rotation happened

    """

    key: str
    last: datetime


class ContainerStats(TypedDict):
    """A container stats object to send to an mqtt topic.

    Attributes
    ----------
    name
        The name of the container
    host
        The docker host
    memory
        Human-readable memory information from docker
    memoryused
        Used memory in MB
    memorylimit
        Memory limit in MB
    netio
        Human-readable network information from docker
    netinput
        Network input in MB
    netinputrate
        Network input rate in MB/s
    netoutput
        Network output in MB
    netoutputrate
        Network output rate in MB/s
    blockinput
        Block (to disk) input in MB
    blockinputrate
        Block (to disk) input rate in MB/s
    blockoutput
        Block (to disk) output in MB
    blockoutputrate
        Block (to disk) output rate in MB/s
    cpu
        The cpu usage by the container in cpu-% (ex.: a docker with 4 cores has 400% cpu available)

    """

    name: str
    host: str
    memory: str
    memoryused: float
    memorylimit: float
    netio: str
    netinput: float
    netinputrate: float
    netoutput: float
    netoutputrate: float
    blockinput: float
    blockinputrate: float
    blockoutput: float
    blockoutputrate: float
    cpu: float


def clientTest():
    client = docker.from_env()
    containers:list[Container]  = client.containers.list(all=True)
    for container in containers:
        if container.status == 'running' :
            stats = container.stats(stream=False)
            memoryused = stats['memory_stats']['usage']/(1024.*1024.)
            memorylimit = stats['memory_stats']['limit']/(1024.*1024.*1024.) # this is correct.
            cpuused = stats['cpu_stats']['cpu_usage']['total_usage']
            cputotal = stats['cpu_stats']['system_cpu_usage']
            cpuperc = 100.*cpuused/cputotal if cputotal != 0 else 0
            # this is an integral, instantaneous need to take a few samples and look in the time interval ...
            # then calculate 
            print(f"Container: {stats['name']}")
            print(f"Memory Used {memoryused:.2f} MB of {memorylimit:.2f} GB Total")
            print(f"cpu usage = {cpuused}/{cputotal}={cpuperc:.4f}%")
            #print(stats['cpu_stats']['system_cpu_usage'])
            #print(stats['memory_stats']['usage'])
            #print(stats['memory_stats']['limit'])
            #print("foo")
            cpu = []
            #
            # The below will take 10 samples from the stream
            #
            
            for _, status in zip(range(10), container.stats(decode=True, stream=True)):
                foo = status['cpu_stats']
                cpu.append([status['cpu_stats']['cpu_usage']['total_usage'], time.time()])
            #print(cpu)
## Subscribe to Docker events
#for event in client.events(decode=True, filters={"type": "container"}):
#    handle_event(event)
# return {c.name: c.status for c in containers}#
MAX_QUEUE_SIZE = 100

class DockerAPITest:
    from datetime import datetime
    docker_events_t: Thread
    docker_stats_t: Thread
    b_stats: bool = False
    b_events: bool = False

    docker_events: Queue[dict] = Queue(maxsize=MAX_QUEUE_SIZE)
    docker_stats: Queue[dict] = Queue(maxsize=MAX_QUEUE_SIZE)
    known_stat_containers: dict[str, ContainerStatsRef] = {}
    last_stat_containers: dict[str, ContainerStats | dict[str, Any]] = {}
    #pending_destroy_operations: dict[str, float] = {}
   ## Create a Docker client
    def __init__(self):
        self.client = docker.from_env()
        self.containers:list[Container]  = self.client.containers.list(all=True)

    def _handle_stats_queue(self) -> None:
        """Check if any event is present in the queue and process it.

        Raises
        ------
        Docker2MqttStatsException
            If anything goes wrong in the processing of the stats

        """
        from datetime import timedelta
        stat_line = ""

        docker_stats_qsize = self.docker_stats.qsize()
        try:
            stat_dict = self.docker_stats.get(block=False)
        except Empty:
            # No data right now, just move along.
            return


        if docker_stats_qsize > 0:
            if stat_dict and len(stat_dict) > 0:
                try:
                    stat = stat_dict
                    stat_line = json.dumps(stat)
                    #print(f"[handle_stats] loaded stat_dict for container : {stat['Name']}")
                    # print(stat)
                    container: str = stat["Name"]

                    if container not in self.known_stat_containers:
                        self.known_stat_containers[container] = ContainerStatsRef(
                            {"key": "", "last": datetime(2020, 1, 1)}
                        )

                        self.last_stat_containers[container] = {}

                    check_date = datetime.now() - timedelta(seconds=1)
                    container_date = self.known_stat_containers[container]["last"]
                    if container_date > check_date:
                        print("[handle_stats] Not processing record, too recent: cont = {container_date} - 20 secs ago {check_date} ")
                        return

                    stat_key = hashlib.md5(stat_line.encode("utf-8")).hexdigest()
                    existing_stat_key = self.known_stat_containers[container]["key"]
                            #"Compare hashes %s %s", stat_key, existing_stat_key
                    if stat_key == existing_stat_key:
                        print( "[handle_stats] Not processing duplicate record:  ")
                        return

                    # print(f"[handle_stats] Processing {stat['Name']} ")
                    self.known_stat_containers[container]["key"] = stat_key
                    self.known_stat_containers[container]["last"] = (datetime.now())

                    # if we need the elapsed time for rates
                    delta_seconds = (
                        self.known_stat_containers[container]["last"] - container_date
                    ).total_seconds()

                    # here calculate the cpu and memory used
                    last_stat = self.last_stat_containers[container]
                    delta_cpu_used = 0
                    delta_total_cpu = 0
                    cpu_percent = 0
                    if len(last_stat) >0 :
                        delta_cpu_used = stat["cpuused"]-last_stat['netinput'] 
                        delta_total_cpu = stat["cputotal"]-last_stat['netoutput']
                        cores = stat['cores']
                        # this now works - needed to add the cores ...
                        cpu_percent = float(delta_cpu_used*cores)/float(delta_total_cpu) if delta_total_cpu > 0 else 0

                    container_stats = ContainerStats(
                        {
                            "name": container,
                            "host": "test",
                            "cpu": cpu_percent,
                            "memory": "foo",
                            "memoryused": stat["memoryused"],
                            "memorylimit": stat["memorylimit"],
                            "netio": "bar",
                            "netinput": delta_cpu_used,
                            "netinputrate": 0,
                            "netoutput": delta_total_cpu,
                            "netoutputrate": 0,
                            "blockinput": 0,
                            "blockinputrate": 0,
                            "blockoutput": 0,
                            "blockoutputrate": 0,
                        }
                    )
                    print(f"[handle_stats] container stats are {container_stats['name']} cpu = {container_stats['netinput']} cpu: {cpu_percent*100:.4f} %")
                    self.last_stat_containers[container] = container_stats

                except Exception as ex:
                    print(f"error {ex}")



    def _start_readline_stats_thread(self) -> None:
        """Start the stats thread."""
        self.docker_stats_t = Thread(
            target=self._run_readline_stats_thread, daemon=True, name="Stats"
        )
        self.docker_stats_t.start()

    def _run_readline_stats_thread(self) -> None:
        """Run docker events and continually read lines from it."""
        try:
            while True:
                for container in self.client.containers.list(all=True): # could filter here ...
                    if container.status == 'running' :
                        stats = container.stats(stream=False)
                        memoryused = stats['memory_stats']['usage']
                        memorylimit = stats['memory_stats']['limit']
                        cpuused = stats['cpu_stats']['cpu_usage']['total_usage']
                        cputotal = stats['cpu_stats']['system_cpu_usage']
                        cores = stats['cpu_stats']['online_cpus']
                        statDict = {
                            "Name":container.name,
                            "memoryused":memoryused,
                            "memorylimit":memorylimit,
                            "cpuused":cpuused,
                            "cputotal":cputotal,
                            "cores":cores
                        }
                        self.docker_stats.put(statDict)
                        print(f"[readline_stats] >>> putting stats for {container.name} in queue: {statDict['Name']} {statDict['memoryused']}")
                sleep(10)
        except Exception as ex:
            print(f"error reading stat data{ex}")

test = DockerAPITest()
test._start_readline_stats_thread()
while True:
    test._handle_stats_queue()
    sleep(2)
    #sleep(30)
