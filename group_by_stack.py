# Agrupar por ação
# Ordernar (temporal)
#  Calcular a MA - Média móvel

# Análise temporal de dados:

from mrjob.job import MRJob

class MovingAvarage(MRJob):
    
    window = 3 #define o tamanho da janela de análise da media movel
    
    def mapper(self, _, line): 
        name, date, value = line.split(',')
        
        yield name, (date, value) #agrupa as informações por ação
        
    def reduce(self, key, values):
        items = list(values)
        
        items.sort() #ordena a lista
        
        total = 0.0
        ma = 0.0
        for i in range(len(items)):
            item = items[i]
            
            total = total + item[i]
            if i >= self.window:
                total = total - items[i - self.window][1]
                            
            quociente = min(i + 1, self.window) #o valor do quociente, deve começar em 1, e ir no maximo até 3. Como o valor começa em zero
            # de inicio, sera sempre o i, até que atinja o valor maior de 3, que sempre levara em consideração o valor da janela. 
            # Serve como alternativa ao if/else para verificação;
            ma = total/quociente
            yield key, (item[0], item[1], ma)
        
if __name__ == '__main__':
    MovingAvarage.run()