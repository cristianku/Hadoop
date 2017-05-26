from mrjob.job import MRJob
from mrjob.step import MRStep


class MRCustomerOrdersSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),

            MRStep(mapper=self.mapper_for_sorting,
                   reducer=self.final_reducer),

        ]

    def mapper(self, key, line):
        (customer, item, order_amount) = line.split(',')
        yield '%04d'%int(customer), float(order_amount)

    def reducer(self, customer, orders_amount_list):
        yield customer, sum(orders_amount_list)


    def mapper_for_sorting(self, customer, total_orders_amount):
        yield '%04.02f'%float(total_orders_amount), customer


    def final_reducer(self, total_orders_amount, customers_list):
        for customer in customers_list:
            yield customer, total_orders_amount


if __name__ == '__main__':
    # please put in the script parameter ( python command line ) : ml-100k/u.data
    #example : python data/customer-orders.csv

    MRCustomerOrdersSorted.run()
