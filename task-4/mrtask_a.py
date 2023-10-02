# Which vendors have the most trips, and what is the total revenue generated by that vendor?
from mrjob.job import MRJob
from mrjob.step import MRStep

class VendorTripAnalyzer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.find_max_reducer),
        ]

    def mapper(self, _, line) :
        if not line.startswith('VendorID'):
            cols = line.split(',')
            yield int(cols[0]), round(float(cols[16]), 2)


    def reducer(self, vendorId, values):
        count, sum = 0, 0

        for val in values:
            count += 1
            sum+=val
        yield None, (count, sum, vendorId)

    def find_max_reducer(self, _, details):
        count, totalAmount, vendorId = max(details)
        yield vendorId, totalAmount

if __name__ == '__main__':
    VendorTripAnalyzer.run()

# python mrtask_a.py input  > mrtask_a_result.txt 