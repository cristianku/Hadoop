from mrjob.job import MRJob

class MRMovieCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, movieID

    def reducer(self, userID, movieID):
        yield userID, sum(movieID)

if __name__ == '__main__':
    MRMovieCounter.run()
