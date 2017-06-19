from dateutil.parser import parse
from dateutil import rrule
import yaml
import operator

class sflydwh(object):
    
    def __init__(self):
        self.event_order = 'ORDER'
        self.event_site_visit = 'SITE_VISIT'
        self.event_customer = 'CUSTOMER'
        self.event_image = 'IMAGE'
        self.event_time = 'event_time'
        self.total_amount = 'total_amount'

    def ingest(self,e, D):
        """
        Functions takes data and converts into dictionary
        Key - Customer ID
        Value - List with data of event types - ORDER, SITE_VISIT, IMAGE
        """
        iter1 = True
        with open(e) as f:
            for line in f.readlines():
                if iter1:
                    iter1 = False
                    data = line.strip()[1:-1]
                    e = yaml.load(data)
                    if self.event_time in e:
                        e[self.event_time] = parse(e[self.event_time])
                    
                    #Segregating CUSTOMER event and other events (ORDER, SITE_VISIT, IMAGE)
                    if e['type'] != self.event_customer:
                        customer_id = e['customer_id']
                    else:
                        customer_id = e['key']
                
                    if customer_id in D:
                        #Ingest data of customer id
                        D[customer_id].append(e)
                    else:
                        #If customer id not present in Dictionary, add new customer id
                        D[customer_id] = [e]
                else:
                    data = line.strip()[:-1]
                    e = yaml.load(data)
                    if self.event_time in e:
                        e[self.event_time] = parse(e[self.event_time])
                    
                    #Segregating CUSTOMER event and other events (ORDER, SITE_VISIT, IMAGE)
                    if e['type'] != self.event_customer:
                        customer_id = e['customer_id']
                    else:
                        customer_id = e['key']
                
                    if customer_id in D:
                        #Ingest data of customer id
                        D[customer_id].append(e)
                    else:
                        #If customer id not present in Dictionary, add new customer id
                        D[customer_id] = [e]
    
    def topXSimpleLTVCustomers(self, x, D, print_info=False):
        """
        Used "https://blog.kissmetrics.com/how-to-calculate-lifetime-value/?wide=1" as reference
        1. Calculated Customer Expenditures per visit -- Used ORDER event for this calculation 
        2. Calculated Number of orders per week
        3. Then, calculated avg. customer value per week (a)
        4. For Customer LifeTime Value, used LTV = 52(a)xt formula
        Assumed that average customer lifespan for Shutterfly is 10 years (t)
        """
        LTV = []    #Initialize LTV list
        for customer_id in D:         
            event_type = []
            for l in D[customer_id]:
                event_type = l[ 'type']
            if self.event_order in event_type:
                order_id = self.event_order
            else:
                order_id = 'ORDER'    
            
            orderDate = []
            for l in D[customer_id]:
                if l['type'] == order_id:
                    orderDate.append(l[self.event_time])
            
            #Count number of weeks when customer puts in order    
            if orderDate and 'ORDER' in event_type:
                orderWeekCount = (rrule.rrule(rrule.WEEKLY, dtstart=min(orderDate), until =max(orderDate) )).count()

                order_data = []
                for l in D[customer_id]:
                    if l['type'] == self.event_order:
                        order_data = [(l['key'], l['event_time'], float(l[self.total_amount].split()[0]))]
              
                orderAmountDic = {}
                for k, event_time, total_amount in order_data:
                    if k not in orderAmountDic:
                        orderAmountDic[k] = (event_time, total_amount)
                    else:
                        if event_time > orderAmountDic[k][0]:
                            # Update order amount
                            orderAmountDic[k] = (event_time, total_amount)
                #Calculate total order for customer by customer ID
                total_order_amounts = sum([orderAmountDic[k][1] for k in orderAmountDic])
                
                #Calculate average customer value per week (a)
                avgCustValuePerWeek = float(total_order_amounts) / orderWeekCount
    
                #Append LTV for each customer ID
                valueLTV= 52 * avgCustValuePerWeek * x
                LTV.append( (customer_id, valueLTV) )
        LTV.sort(reverse=True, key=operator.itemgetter(1))
        return LTV[:x]



customerLTV = {}
print_info = True
obj = sflydwh()
obj.ingest("./input.txt", customerLTV)
topCustomers = obj.topXSimpleLTVCustomers(10, customerLTV)
output_file = "./output.txt"
with open(output_file, 'w') as f:
    f.write('Following is the list of non-zero LTV value customer -\n')
    for x in topCustomers:
        f.write(x[0] + ', ' + str(x[1]) + '\n')