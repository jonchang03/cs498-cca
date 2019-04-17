# import os
# from os.path import join
from time import sleep

# from streamparse import Spout
import storm

class FileReaderSpout(storm.Spout):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._complete = False

        storm.logInfo("Spout instance starting...")

        # TODO:
        # Task: Initialize the file reader
        fname = "/tmp/data.txt"
        self.fr = open(fname, "r") # add file reader here
        # End

    def nextTuple(self):
        # TODO:
        # Task 1: read the next line and emit a tuple for it
        for line in self.fr.readlines():
            # Emit a random sentence
            # storm.logInfo("Emiting %s" % line)
            line = line.strip()
            storm.emit([line])

        # Task 2: don't forget to sleep for 1 second when the file is entirely read to prevent a busy-loop
        sleep(1)
        self._complete = True
        # End
        


# Start the spout when it's invoked
FileReaderSpout().run()
