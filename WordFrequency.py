from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile (r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):

        words = WORD_REGEXP.findall(line)
        # print (" **************** ")
        # print (line)
        # print (" **************** ")
        # print ("  ")
        for word in words:
             yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()

#python WordFrequency.py data/Book.txt
