import math


def to_be_run_under_test(count: int):
    for _ in range(5 if count == 1 else 20):
        v = []
        for index in range(count):
            v.append(math.exp(index/1000.0))
