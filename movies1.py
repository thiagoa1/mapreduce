from mrjob.job import MRJob
from mrjob.step import MRStep

class Movies1(MRJob):
    
    def mapper(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1
    
    def reducer(self, movie_id, values):
        yield None, (sum(values), movie_id)

    #def mapper_sort(self, _, line):

    def reducer_sort(self, value_none, values):
        #yield values.sort(key=lambda tup: tup[1])
        valuesList = list(values)
        sortedValues = sorted(valuesList)
        for value in sortedValues:
            yield value[0], value[1]    

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    Movies1.run()