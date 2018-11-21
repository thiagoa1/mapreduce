from mrjob.job import MRJob
from mrjob.step import MRStep

class Movies1(MRJob):
    
    def mapper(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1
    
    def reducer(self, movie_id, values):
        yield None, (sum(values), movie_id)

    def reducer_sort(self, value_none, values):
        valuesList = list(values)
        sortedValues = sorted(valuesList)
        for value in sortedValues:
            yield value[1], value[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    Movies1.run()