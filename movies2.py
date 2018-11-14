from mrjob.job import MRJob
from mrjob.step import MRStep

class Movies2(MRJob):
    
    def mapper(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1
    
    def reducer(self, movie_id, values):
        yield None, (sum(values), movie_id)

    def reducer_sort(self, value_none, values):
        valuesList = list(values)
        descSortedValues = list(reversed(sorted(valuesList)))
        result = descSortedValues[:10]
        for value in result:
            yield value[0], value[1]    

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    Movies2.run()