from mrjob.job import MRJob

class OrdersByCostumer(MRJob):
    def mapper(self, _, line):
        tokens = line.split(',')

        yield tokens[0], float(tokens[2])

    def reducer(self, key, value):
        yield float(key), sum(value)

if __name__ == '__main__':
    OrdersByCostumer.run()

# start cmd => python py archive csv archive
# start cmd and generate file=> python py_archive csv_archive >> output_file_name 