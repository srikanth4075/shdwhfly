from datetime import datetime
import re, os, sys, json
from functools import reduce

class BaseEvent(object):
   """
   this is the base event class that all other events must extend
   """

   def __init__(self, event, keys):
       """
       constructor
       :param event: event object
       :param keys: list of keys to expect in the event object
       :return:
       """
       self.key = event['key']
       self.event_time = datetime.strptime(event['event_time'][:19], '%Y-%m-%dT%H:%M:%S')
       self._validate_event(event, keys)
       self.event = event

   def __repr__(self):
       return str(self.event)

   @staticmethod
   def _validate_event(event, keys):
       """
       :param event: event object
       :param keys: list of keys in the input object to be read
       :return: True if all keys are found in event else False
       """
       assert reduce(lambda it1, it2: it1 and it2, [x in event for x in list(set(["key", "event_time"] + keys))],
                     True), "event %s doesn't conform to expected schema"

class Customer(BaseEvent):

   def __init__(self, event, keys=["last_name", "adr_city", "adr_state"]):
       """

       :param event: event object
       :param keys: list of keys in the input object to be read
       :return:
       """
       super(Customer, self).__init__(event, keys)
       self.last_name = event['last_name']
       self.adr_city = event['adr_city']
       self.adr_state = event['adr_state']

class SiteVisit(BaseEvent):

   def __init__(self, event, keys=['customer_id', 'tags']):
       """

       :param event: event object
       :param keys: list of keys in the input object to be read
       :return:
       """
       super(SiteVisit, self).__init__(event, keys)
       self.customer_id = event['customer_id']
       self.tags = event['tags']

class ImageUpload(BaseEvent):

   def __init__(self, event, keys=['customer_id', 'camera_make', 'camera_model']):
       """

       :param event: event object
       :param keys: list of keys in the input object to be read
       :return:
       """
       super(ImageUpload, self).__init__(event, keys)
       self.customer_id = event['customer_id']
       self.camera_make = event['camera_make']
       self.camera_model = event['camera_model']

class Order(BaseEvent):

   def __init__(self, event, keys=['customer_id', 'total_amount']):
       """

       :param event: event object
       :param keys: list of keys in the input object to be read
       :return:
       """
       super(Order, self).__init__(event, keys)
       self.customer_id = event['customer_id']
       self.total_amount = Order._parse_total_amount(event['total_amount'])

   @staticmethod
   def _parse_total_amount(in_str):
       num, decimal = re.compile(r'([\d]+)(\.[0-9]+)?').findall(in_str)[0]
       if decimal:
           return float('%s%s' % (num, decimal))
       else:
           return int(num)

class DataStore(object):

   def __init__(self):
       self._customers = {}
       self._site_visits = {}
       self._image_uploads = {}
       self._orders = {}

   def __repr__(self):
       return str({"customer": self._customers, "site_visits": self._site_visits,
                   "image_upload": self._image_uploads, "orders": self._orders})

   def get_customers(self):
       """

       :return: list of customer
       """
       return self._customers.values()

   def get_site_visits(self, customer_id):
       """

       :param customer_id: customer id
       :return: list of site-visits for the given customer
       """
       return self._site_visits.get(customer_id, [])

   def get_orders(self, customer_id):
       """

       :param customer_id: customer id
       :return: liist of orders for the given customer
       """
       return self._orders.get(customer_id, [])

   def add_customer(self, event):
       """
       ingest new customer
       :param event: customer event object
       """
       assert isinstance(event, Customer), "event must be of type Customer"
       self._customers[event.key] = event

   def add_site_visit(self, event):
       """
       ingest new site visit object
       :param event:
       """
       isinstance(event, SiteVisit), "event must be of type SiteVisit"
       self._site_visits[event.customer_id] = self._site_visits.get(event.customer_id, []) + [event]

   def add_image_uploads(self, event):
       """
       ingest new image upload object
       :param event:
       :return:
       """
       isinstance(event, ImageUpload), "event must be of type ImageUpload"
       self._image_uploads[event.customer_id] = self._image_uploads.get(event.customer_id, []) + [event]

   def add_order(self, event):
       """
       ingest new order event
       :param event:
       :return:
       """
       isinstance(event, Order), "event must be of type Order"
       self._orders[event.customer_id] = self._orders.get(event.customer_id, []) + [event]

def ingest(event, data_store):
   """
   ingest event into data store
   :param event: event object
   :param data_store: data store
   :return:
   """
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
    """

    :param x: no of customers to return
    :param data_store:
    :return:
    """
    result = sorted([(c, calculateLTV(data_store.get_site_visits(c.key), data_store.get_orders(c.key))) for c in
                     data_store.get_customers()], key=lambda ob: ob[1],reverse=True)
    return list(map(lambda y: y[0], result))[:x]


def calculateLTV(site_visits, orders):
    """
    
    :param site_visits: list of site visits for a customer
    :param orders: list of orders for a customer
    :return: calculated LTV
    """
    AVERAGE_LIFE_SPAN = 10

    def bucket_week_data(data):
        return [((year, week), item) for ((year, week, _), item) in
                [(x.event_time.isocalendar(), x) for x in data]]
    
    
    site_visits_per_week = reduce(lambda acc, key: dict(list(acc.items()) + [(key[0], acc.get(key[0], 0) + 1)]),
                                  bucket_week_data(site_visits), dict())
    avg_visit_per_week = sum(site_visits_per_week.values()) / len(site_visits_per_week)
    exp_per_visit = sum([x.total_amount for x in orders]) / len(site_visits)
    return 52 * (exp_per_visit * avg_visit_per_week) * AVERAGE_LIFE_SPAN

if __name__ == '__main__':

   with open('/Users/srmc/Desktop/Spark/code-challenge-master/sample_input/events.txt','r') as in_file:
       data_store = DataStore()
       events = json.load(in_file)
       for item in events:
           ingest(item, data_store)
           
       ip = int(input('Enter a Number to get top list: '))
       results = topXSimpleLTVCustomers(ip, data_store)
       print('Customer_key|Last_name')
       for items in results:
            print(items.key + "|" + items.last_name)
