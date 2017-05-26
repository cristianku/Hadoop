from mrjob.job import MRJob

class MRMovieCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, movieID

    def reducer(self, userID, movieID):
        yield userID, sum(movieID)

if __name__ == '__main__':
    # please put in the script parameter ( python command line ) : ml-100k/u.data
    #example : python ml-100k/u.data

    MRMovieCounter.run()
