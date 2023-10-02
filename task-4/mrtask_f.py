# How does revenue vary over time?
# Calculate the average trip revenue per month - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class TripRevenueAnalyzer(MRJob):
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
            start_datetime = self.parse_datetime(cols[1])
            amount = float(cols[16])
            month_name = datetime.strptime(str(start_datetime.month), '%m').strftime('%B')

            # Determine if it's day or night (you can adjust the threshold as needed)
            is_day = 6 <= start_datetime.hour < 18
            is_weekend = start_datetime.weekday() >= 5  # Saturday (5) and Sunday (6) are weekend days

            key = (month_name, 'weekend' if is_weekend else 'weekday', 'day' if is_day else 'night')

            # Emit key-value pairs for grouping and aggregation
            yield key, amount

    def reducer(self, key, values):
        total_revenue = 0
        total_trips = 0
        for amount in values:
            total_revenue += amount
            total_trips += 1
        yield None, (key, (total_revenue / total_trips, total_trips))
    
    def final_sort(self, _, values):
        sorted_values = sorted(values)

        for val in sorted_values:
            yield val[0], val[1]


if __name__ == '__main__':
    TripRevenueAnalyzer.run()

# python mrtask_f.py input  > mrtask_f_result.txt 