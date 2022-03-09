#!/usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class Demo2(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                   reducer=self.reducer2),
            MRStep(mapper=self.mapper3)
        ]

    def mapper1(self, _, line):
            data = line.split(",")
            if not (data[0] =='ip'):
                date = data[1]
                ip = data[0]
                # cheking for Non valid extention
                if (data[6][0] == '.'):
                    fileName = data[5] + data[6]
                else:
                    fileName = data[6]
            yield (ip+','+fileName+','+date, 1)

    def reducer1(self, key, value):
         yield (key, 1)

    def mapper2(self, key, value):
        data = key.split(',')
        ip = data[0]
        filename = data[1]
        yield(ip+','+filename,1)

    def reducer2(self, key, value):
        yield(key, sum(value))

    def mapper3(self, key, value):
        if(value>1):
            data = key.split(",")
            ip = data[0]
            filename = data[1]
            yield(ip, filename)

if __name__ == '__main__':
    Demo2.run()