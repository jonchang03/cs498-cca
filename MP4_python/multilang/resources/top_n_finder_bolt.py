import heapq
from collections import Counter

import storm


class TopNFinderBolt(storm.BasicBolt):
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        # storm.logInfo("Counter bolt instance starting...")

        # TODO:
        # Task: set N
        self._N = 10 # figure out how to get from flux
        # End

        # Hint: Add necessary instance variables and classes if needed
        self._counter = Counter()

    def process(self, tup):
        '''
        TODO:
        Task: keep track of the top N words
        Hint: implement efficient algorithm so that it won't be shutdown before task finished
              the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
        '''
        word = tup.values[0]
        count = int(tup.values[1])

        # if word is not None:
        # if word doesn't exist, add to counter
        if word not in self._counter:
            self._counter[word] = count
        # only update if count is greater than current count
        elif count > self._counter[word]:
            self._counter[word] = count

        top_n_values = self._counter.most_common(self._N)

        # Emit the topN and count
        # field-value pairs ("top-N", {top N words string}) 
        top_n = "top-N"
        # top_n_words = str(top_n_values) # convert dict to string for now
        
        # format for emit
        top_n_list = [x[0] for x in top_n_values]
        top_n_words = ", ".join(top_n_list)
        storm.emit([top_n, top_n_words]) 

        # End


# Start the bolt when it's invoked
TopNFinderBolt().run()
