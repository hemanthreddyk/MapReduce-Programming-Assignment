# Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.
from mrjob.job import MRJob
from mrjob.step import MRStep

class LocationwiseTipsToRevenueAnalyzer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer=self.final_sort)
        ]


    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            cols = line.split(',')
            amount = float(cols[16])
            tips = float(cols[13])
            
            yield int(cols[7]), (amount, tips)

    def combiner(self, pickupLocationId, tripDetails):
        totalTipAmount, totalRevenue = 0, 0
        for t in tripDetails:
            totalRevenue += t[0]
            totalTipAmount += t[1]

        yield pickupLocationId, (totalRevenue, totalTipAmount)  

    def reducer(self, pickupLocationId, tripDetails):
        totalTipAmount, totalRevenue = 0, 0
        for revenue, tip in tripDetails:
            totalTipAmount += tip
            totalRevenue += revenue
        ratio = totalTipAmount/totalRevenue

        yield None, (ratio, pickupLocationId)

    def final_sort(self, _, tripDetails):
        sorted_trip_details = sorted(tripDetails, reverse=True)

        for val in sorted_trip_details:
            yield val[1], val[0]

if __name__ == '__main__':
    LocationwiseTipsToRevenueAnalyzer.run()

# python mrtask_e.py input  > mrtask_e_result.txt 