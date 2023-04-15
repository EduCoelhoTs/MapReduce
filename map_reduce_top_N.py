from mrjob.job import MRJob, MRStep

class TopN(MRJob):
    top = [] #criação de uma variavel geral, para guardar o estado dos dados
    def mapper(self, _, line):
        weight, id, name = line.split(',')
        weight = float(weight)
        
        self.top.append((weight, name)) #guarda os dados em uma lista
        if(len(self.top) > 5): #se a lista for maior que 5, ordena a lista do menor para o maior, e exclui o menor;
            self.top.sort()
            self.top.pop(0)

    def reducer_init(self):
        for item in self.top:
            yield (None, item)
    
    def reducer(self, key, values):
        items = list(values)
        
        items.sort(reverse=True)
        for i in range(5):
            item = items[i]
            yield item[1], item[0]
            
    def steps(self):
        return[
            MRStep(mapper=self.mapper, reducer_init=self.reducer_init),
            MRStep(reducer=self.reducer)
        ]
        
if __name__ == '__main__':
    TopN.run()
    
    # command: $ python map_reduce_top_N.py cats.csv >> cats.result.csv