# fazendo o reduce por faixa de idade:
from mrjob.job import MRJob
# Faixas de idade:
# 18-25
# 26-32
# 33-40
# 41-48
# 49-69

class friendsByAge(MRJob):
    def mapper(self, _, line):
        id, name, age, amount = line.split(',')

        key = ''
        age = int(age)
        if age >= 18 and age <= 25:
           key = '18-25'
        elif age >= 26 and age <= 32:
           key = '26-32'
        elif age >= 33 and age <= 40:
           key = '33-40'
        elif age >= 41 and age <= 48:
           key = '41-48'
        else:
           key = '49-69'

        yield key, float(amount)

    def reducer(self, key, values):
        items = list(values)
        avg = sum(items)/len(items)

        yield key,(avg, max(items), min(items))
# retornando (chave, tupla(media, valor max, valor min))

if __name__ == '__main__':
    friendsByAge.run()