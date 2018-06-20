import datetime
import json
import time


class Payload:

    def __init__(self):
        self.payloadObj = {}
        self.start = 0
        self.end = 0
        self.units = 0
        self.source = ''
        self.id = 0
        self.event = ''
        self.eventSourceTimestamp = 0

    def get_payload_data(self):
        return json.dumps({
            'payload': self.get_payload(),
            'correlation_id': {
                'source': self.get_source(),
                'start': self.get_start(),
                'units': self.get_units(),
                'end': self.get_end(),
            },
            'eid': self.get_eid(),
            'id': self.get_id(),
            'event': self.get_event(),
            'event_source_timestamp': self.get_event_source_timestamp(),
            'timestamp': time.time()
        })

    #
    # payload Object
    #
    def set_payload(self, name, ids):
        if name in self.payloadObj:
            self.payloadObj[name] += ids
        else:
            self.payloadObj[name] = ids

    def get_payload(self, name=''):
        if not name:
            return self.payloadObj
        else:
            return self.payloadObj[name] if name else self.payloadObj

    #
    # start
    #
    def set_start(self, start):
        self.start = start

    def get_start(self):
        return self.start

    #
    # end
    #
    def set_end(self, end):
        self.end = end

    def get_end(self):
        return self.end

    #
    # units
    #
    def set_units(self, units):
        self.units = units

    def get_units(self):
        return self.units

    def increment_units(self):
        self.units += 1

    def decrement_units(self):
        self.units -= 1

    def increase_units(self, amount):
        self.units += amount

    def decrease_units(self, amount):
        self.units -= amount

    #
    # source
    #
    def set_source(self, source):
        self.source = source

    def get_source(self):
        return self.source

    #
    # id
    #
    def set_id(self, id):
        self.id = id

    def get_id(self):
        return self.id

    #
    # event
    #
    def set_event(self, event):
        self.event = event

    def get_event(self):
        return self.event

    #
    # event_source_timestamp
    #
    def set_event_source_timestamp(self, eventSourceTimestamp):
        self.eventSourceTimestamp = eventSourceTimestamp if eventSourceTimestamp else time.time()

    def get_event_source_timestamp(self):
        return self.eventSourceTimestamp if self.eventSourceTimestamp else time.time()

    #
    # eid
    #
    def get_eid(self):
        return datetime.datetime.now().strftime('z/%Y/%m/%d/%H/%M/%S/') + str(int(time.time())) + '-0000000'