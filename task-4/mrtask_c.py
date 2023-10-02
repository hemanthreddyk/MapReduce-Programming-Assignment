# What are the different payment types used by customers and their count? The final results should be in a sorted format.
from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypeStatistics(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_sort)
        ]

    def get_payment_type(self, payment_type_code):
        payment_type_mapping = {
            1: 'Credit card',
            2: 'Cash',
            3: 'No charge',
            4: 'Dispute',
            5: 'Unknown',
        }

        payment_type = payment_type_mapping.get(payment_type_code, 'Voided trip')
        return payment_type

    def mapper(self, _, line) :
        if not line.startswith('VendorID'):
            cols = line.split(',')
            payment_type_code = int(cols[9])
            yield self.get_payment_type(payment_type_code), 1

    def reducer(self, payment_type, count):
        yield None, (sum(count), payment_type)

    def final_sort(self, _, values):
        sorted_values = sorted(values, reverse=True)

        for val in sorted_values:
            yield val[1], val[0]

if __name__ == '__main__':
    PaymentTypeStatistics.run()

# python mrtask_c.py input  > mrtask_c_result.txt 