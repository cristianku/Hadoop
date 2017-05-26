from mrjob.job import MRJob
from mrjob.step import MRStep

import re

WORD_REGEXP = re.compile (r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep (mapper  = self.mapper_get_words,
                    reducer = self.reducer_count_words),

            MRStep(mapper=self.mapper_make_counts_key,
                   reducer=self.reducer_output_words),

        ]
    def mapper_get_words(self, _, line):

        words = WORD_REGEXP.findall(line)
        for word in words:
             yield word.lower(), 1

    def reducer_count_words(self, key, values):
        yield key, sum(values)


    def mapper_make_counts_key(self, key, values_sum):
        yield '%04d'%int(values_sum), key


    def reducer_output_words(self, key, values):
        for word in values:
            yield word, key


if __name__ == '__main__':
    MRWordFrequencyCount.run()

#python WordFrequencySorted.py data/Book.txt
