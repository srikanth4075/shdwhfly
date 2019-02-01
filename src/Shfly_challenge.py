from datetime import datetime
import re

class BaseEvent(object):

    def _init_(self, event, keys):
        self.key = event['key']
        self.event_time = datetime.strptime(event['event_time'][:19], '%Y-%m-%dT%H:%M:%S')
        self._validate_event(event, keys)
        self.event = event

    def _repr_(self):
        return str(self.event)

    @staticmethod
    def _validate_event(event, keys):
        """
        :param event:
        :param keys:
        :return: True if all keys are found in event else False
        """
        assert reduce(lambda it1, it2: it1 and it2, [x in event for x in list(set(["key", "event_time"] + keys))],
                      True), "event %s doesn't conform to expected schema"


class Customer(BaseEvent):

    def _init_(self, event):
        super(Customer, self)._init_(event, ["last_name", "adr_city", "adr_state"])
        self.last_name = event['last_name']
        self.adr_city = event['adr_city']
        self.adr_state = event['adr_state']


class SiteVisit(BaseEvent):

    def _init_(self, event):
        super(SiteVisit, self)._init_(event, ['customer_id', 'tags'])
        self.customer_id = event['customer_id']
        self.tags = event['tags']


class ImageUpload(BaseEvent):

    def _init_(self, event):
        super(ImageUpload, self)._init_(event, ['customer_id', 'camera_make', 'camera_model'])
        self.customer_id = event['customer_id']
        self.camera_make = event['camera_make']
        self.camera_model = event['camera_model']


class Order(BaseEvent):

    def _init_(self, event):
        super(Order, self)._init_(event, ['customer_id', 'total_amount'])
        self.customer_id = event['customer_id']
        self.total_amount = Order._parse_total_amount(event['total_amount'])

    @staticmethod
    def _parse_total_amount(str):
        return int(re.compile('\d+').findall(str)[0])


class DataStore(object):

    def _init_(self):
        self._customers = {}
        self._site_visits = {}
        self._image_uploads = {}
        self._orders = {}

    def _repr_(self):
        return str({"customer": self._customers, "site_visits": self._site_visits,
                "image_upload": self._image_uploads, "orders": self._orders})

    def get_customers(self):
        return self._customers.values()

    def get_site_visits(self, customer_id):
        return self._site_visits.get(customer_id, [])

    def get_orders(self, customer_id):
        return self._orders.get(customer_id, [])

    def add_customer(self, event):
        isinstance(event, Customer), "event must be of type Customer"
        self._customers[event.key] = event

    def add_site_visit(self, event):
        isinstance(event, SiteVisit), "event must be of type SiteVisit"
        self._site_visits[event.customer_id] = self._site_visits.get(event.customer_id, []) + [event]

    def add_image_uploads(self, event):
        isinstance(event, ImageUpload), "event must be of type ImageUpload"
        self._image_uploads[event.customer_id] = self._image_uploads.get(event.customer_id, []) + [event]

    def add_order(self, event):
        isinstance(event, Order), "event must be of type Order"
        self._orders[event.customer_id] = self._orders.get(event.customer_id, []) + [event]


def ingest(event, data_store):
    assert 'type' in event, "type field is missing the event"
    if event['type'] == 'CUSTOMER':
        data_store.add_customer(Customer(event))
    elif event['type'] == 'SITE_VISIT':
        data_store.add_site_visit(SiteVisit(event))
    elif event['type'] == 'IMAGE':
        data_store.add_image_uploads(ImageUpload(event))
    elif event['type'] == 'ORDER':
        data_store.add_order(Order(event))
    else:
        raise ValueError("unknown type %s" % event['type'])


def topXSimpleLTVCustomers(x, data_store):
    result = sorted([(x, calculateLTV(data_store.get_site_visits(x.key), data_store.get_orders(x.key))) for x in
                   data_store.get_customers()], key=lambda x: x[1])
    return result

def calculateLTV(site_visits, orders):
    AVERAGE_LIFE_SPAN_IN_WEEKS = 520

    def bucket_week_data(data):
        return [((year, week), item) for ((year, week, _), item) in
                map(lambda x: (x.event_time.isocalendar(), x), data)]

    site_visits_per_week = reduce(lambda acc, (key, _): dict(acc.items() + [(key, acc.get(key, 0) + 1)]),
                                  bucket_week_data(site_visits), dict())
    avg_visit_per_week = sum(site_visits_per_week.values()) / len(site_visits_per_week)
    exp_per_visit = sum([x.total_amount for x in orders]) / len(site_visits)
    return 52 * (exp_per_visit * avg_visit_per_week) * AVERAGE_LIFE_SPAN_IN_WEEKS