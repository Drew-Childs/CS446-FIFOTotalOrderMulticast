#!/bin/python3

# Author name: Drew Childs


from simulator import *
from queue import Queue

# Multicast events for the driver
multicast_events = [
    # (time, message_id, sending host_id, message/payload)
    (10, "M1", 1, "Januray"),
    (20, "M2", 1, "February"),
    (30, "M3", 1, "March"),
    (10, "M4", 2, "One"),
    (20, "M5", 2, "Two"),
    (30, "M6", 2, "Three"),
]


class Host(Node):
    def __init__(self, sim, host_id):
        Node.__init__(self, sim, host_id)
        self.host_id = host_id
        self.gmembers = []

        self.deliverySequence = {}
        self.holdBackQueue = {}
        self.deliveryQueue = Queue()

        self.receivedHoldBackQueue = {}
        self.deliveryNumber = 0


    def initialize(self):
        myDict = {}

        # determines order of what messages will be deliverd by which nodes based on time stamp
        if self.node_id == 0:
            for item in multicast_events:
                time = 0
                while True:
                    if time == item[0]:
                        if item[2] in myDict:
                            beans = myDict[item[2]]
                            beans.append(item[1])
                            myDict[item[2]] = beans
                        else:
                            myDict[item[2]] = [item[1]]
                        break
                    time += 1

        self.deliverySequence = myDict


    def multicast(self, time, message_id, message_type, payload):
        # Multicast message to the group
        print(f"Time {time}:: {self} SENDING mulitcast message [{message_id}]")

        # Create message and send to all members of the group including itself
        for to in self.gmembers:
            mcast = Message(message_id, self, to, message_type, payload)
            self.send_message(to, mcast)


    def receive_message(self, frm, message, time):
        # Adding to appropriate queue
        if message.mtype == "DELIVER":
            self.receivedHoldBackQueue[message.payload] = message.message_id
        else:
            print(f"Time {time}:: {self} RECEIVED message [{message.message_id}] from {frm}")
            self.holdBackQueue[message.message_id] = [frm, message, time]

        # determine delivery order for sequencer
        if self.host_id == 0:
            if frm.host_id in self.deliverySequence:
                deliveryList = self.deliverySequence[frm.host_id]
                while True:
                    if len(deliveryList) != 0 and deliveryList[0] in self.holdBackQueue:
                        self.deliveryQueue.put(self.holdBackQueue.pop(deliveryList[0]))
                        self.deliverySequence[frm.host_id].remove(deliveryList[0])
                    else:
                        break
        
        # determine if received messages can be delivered by checking against holdBackQueue and if allowed to deliver from sequencer
        if message.message_id in self.holdBackQueue:
            while True:
                if self.deliveryNumber in self.receivedHoldBackQueue and self.receivedHoldBackQueue[self.deliveryNumber] in self.holdBackQueue:
                    self.deliveryQueue.put(self.holdBackQueue.pop(self.receivedHoldBackQueue[self.deliveryNumber]))
                    self.deliveryNumber += 1
                    pass
                else:
                    break

        # always deliver if there was anything added to queue
        while True:
            if self.deliveryQueue.empty():
                break

            # deliver message
            item = self.deliveryQueue.get()
            self.deliver_message(item[2], item[1])

            # notify other nodes to deliver message
            if self.host_id == 0:
                for to in self.gmembers:
                    mcast = Message(item[1].message_id, self, to, "DELIVER", self.deliveryNumber)
                    self.send_message(to, mcast)
                self.deliveryNumber += 1


    def deliver_message(self, time, message):
        print(f"Time {time:4}:: {self} DELIVERED message [{message.message_id}] -- {message.payload}")


# Driver: you DO NOT need to change anything here
class Driver:
    def __init__(self, sim):
        self.hosts = []
        self.sim = sim

    def run(self, nhosts=3):
        for i in range(nhosts):
            host = Host(self.sim, i)
            self.hosts.append(host)

        for host in self.hosts:
            host.gmembers = self.hosts
            host.initialize()

        for event in multicast_events:
            time = event[0]
            message_id = event[1]
            message_type = "DRIVER_MCAST"
            host_id = event[2]
            payload = event[3]
            self.sim.add_event(
                Event(
                    time,
                    self.hosts[host_id].multicast,
                    time,
                    message_id,
                    message_type,
                    payload,
                )
            )


def main():
    # Create simulation instance
    sim = Simulator(debug=False, random_seed=234)

    # Start the driver and run for nhosts (should be >= 3)
    driver = Driver(sim)
    driver.run(nhosts=5)

    # Start the simulation
    sim.run()


if __name__ == "__main__":
    main()
