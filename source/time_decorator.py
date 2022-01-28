from time import time


def time_method_decorator(function):
    def wrapper(self, *args):
        start = time()
        return_value = function(self, *args)
        print(f"LOG: {function.__repr__().split()[1]} total time in minutes: {(time() - start) / 60}\n")
        return return_value
    return wrapper
