import time
import copy


class Timer:

    count: int = 0

    def __init__(self):
        Timer.count += 1
        self.start_time = time.time()
        # Default values for end_time and duration in case the stopper has not been stopped yet
        self.end_time: time.time = -1
        self.duration: time.time = -1

    def stop(self) -> time.time:
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        return self.end_time

    def lap(self) -> time.time:
        return time.time() - self.start_time

    def restart(self):
        self.start_time = time.time()
        return self.start_time

    def fork(self) -> object:
        """

        :rtype: object
        """
        Timer.count += 1
        return copy.deepcopy(self)
