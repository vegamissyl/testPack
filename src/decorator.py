import time

def checkTime(method):
    def inner(ref):
        start = time.time()
        res = method(ref)
        end = time.time()
        print(f"temps de calcul : {end - start} sec \n")
        return res
    return inner