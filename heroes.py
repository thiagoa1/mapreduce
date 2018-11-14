from mrjob.job import MRJob
from mrjob.step import MRStep

class Heroes(MRJob):
    
    def mapper(self, _, line):
        data_line = line.split(' ')
        hero_id = data_line[0]
        friends_count = len(data_line) - 2
        yield hero_id, friends_count
    
    def reducer(self, hero_id, values):
        yield hero_id, sum(values)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    reducer=self.reducer),
            MRStep(
                reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    Heroes.run()