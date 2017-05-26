from mrjob.job import MRJob

class MRCustomerOrders(MRJob):
    def mapper(self, key, line):
        (customer, item, order_amount) = line.split(',')
        yield '%04d'%int(customer), float(order_amount)

    def reducer(self, key, values_list):
        yield key, sum(values_list)

if __name__ == '__main__':
    # please put in the script parameter ( python command line ) : ml-100k/u.data
    #example : python data/customer-orders.csv

    MRCustomerOrders.run()
