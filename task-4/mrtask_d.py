# What is the average trip time for different pickup locations?
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class LocationTripTimeStatistics(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_sort)
        ]

    def parse_datetime(self, datetime_str):
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('invalid date format')

    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            cols = line.split(',')
            pickup_time = cols[1]
            dropoff_time = cols[2]

            diff = self.parse_datetime(dropoff_time) - self.parse_datetime(pickup_time)
            
            yield int(cols[7]), diff.seconds

    def reducer(self, pickupLocationId, tripTime):
        totalTime, length = 0, 0 
        for t in tripTime:
            totalTime += t
            length += 1

        yield None, (pickupLocationId, totalTime/length)

    def final_sort(self, _, tripDetails):
        sorted_details = sorted(tripDetails)

        for val in sorted_details:
            yield val[0], val[1]

if __name__ == '__main__':
    LocationTripTimeStatistics.run()

# python mrtask_d.py input  > mrtask_d_result.txt 