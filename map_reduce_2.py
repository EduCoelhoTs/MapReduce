from mrjob.job import MRJob

class friendsByAge(MRJob):
    def mapper(self, _, line):
        id, name, age, amount = line.split(',')
        # colunas = dados a serem recebidos
        yield age, float(amount)

    def reducer(self, key, values):
        items = list(values)
        avg = sum(items)/len(items)

        yield key,(avg, max(items), min(items))
# retornando (chave, tupla(media, valor max, valor min))

if __name__ == '__main__':
    friendsByAge.run()