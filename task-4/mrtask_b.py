# Which pickup location generates the most revenue?
from mrjob.job import MRJob
from mrjob.step import MRStep

class RevenueByPickupLocationAnalyzer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.find_max_reducer),
        ]

    def mapper(self, _, line) :
        if not line.startswith('VendorID'):
            cols = line.split(',')
            yield int(cols[7]), round(float(cols[16]), 2)


    def reducer(self, pickupLocationId, revenues):
        yield None, (round(sum(revenues), 2), pickupLocationId)

    def find_max_reducer(self, _, revenues):
        maxRevenue, pickupLocationId = max(revenues)
        yield pickupLocationId, maxRevenue

if __name__ == '__main__':
    RevenueByPickupLocationAnalyzer.run()

# python mrtask_b.py input  > mrtask_b_result.txt 