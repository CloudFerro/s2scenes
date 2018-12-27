import time
import copy


class Timer:

    count = 0

    def __init__(self):
        Timer.count += 1
        self.start_time = time.time()
        # Default values for end_time and duration in case the stopper has not been stopped yet
        self.end_time = -1
        self.duration = -1

    def stop(self):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        return self.end_time

    def lap(self):
        return time.time() - self.start_time

    def restart(self):
        self.start_time = time.time()
        return self.start_time

    def fork(self):
        """

        :rtype: object
        """
        Timer.count += 1
        return copy.deepcopy(self)
