from mrjob.job import MRJob
from mrjob.step import MRStep

class Topn(MRJob):      
#Esta é a classe Topn que herda da classe MRJob do módulo mrjob. É um exemplo de como definir um trabalho em MapReduce em Python usando o mrjob. O objetivo do trabalho é encontrar os cinco maiores pesos em um conjunto de dados de nomes e pesos, e retornar os nomes correspondentes em ordem decrescente por peso.

    top = []
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init),
            MRStep(reducer=self.reducer)  
        ]     
#O atributo top é uma lista vazia que será usada para manter o top-5 pesos e nomes do código atual.
#O método steps retorna uma lista de etapas que compõem o trabalho em MapReduce. O trabalho tem duas etapas, a primeira com um mapper e uma função reducer_init, e a segunda com apenas uma função reducer.
#O primeiro MRStep recebe o mapper mapper e a função reducer_init. O mapper é responsável por processar cada linha do arquivo de entrada, que contém um peso, um id e um nome, e adicioná-los na lista top do objeto Topn. Se a lista top tiver mais de 5 itens, o menor item é removido. A função reducer_init é responsável por emitir os elementos atuais da lista top quando o reducer é inicializado.
#O segundo MRStep possui apenas o reducer reducer. O reducer recebe todas as tuplas emitidas pelo mapper na primeira etapa e cria uma lista de valores. Em seguida, essa lista é ordenada em ordem decrescente e os 5 maiores valores são emitidos usando a função yield, juntamente com seus nomes correspondentes.
    
    def mapper(self, __, line):
        weight, id, name = line.split(',')
        weight = float(weight)
#O método mapper() é responsável por processar cada linha de entrada do arquivo. Nesse caso, a entrada é uma string com três valores separados por vírgula (weight, id, name). A primeira linha da função mapper() extrai os três valores separando-os pela vírgula usando o método split().
#Depois, a variável weight é convertida para um valor de ponto flutuante (float) usando a função float(). O valor id e o valor name são ignorados, pois não são necessários para a análise.
#A saída do método mapper() é uma tupla em que o primeiro valor é a chave (__ é um placeholder que indica que a chave não é importante neste caso), e o segundo valor é uma tupla com o valor de weight e name.
        
        self.top.append((weight, name))
        if len(self.top) > 5:
            self.top.sort()
            self.top.pop(0)
#Essa parte do código é responsável por manter uma lista ordenada dos cinco maiores pesos encontrados até o momento no conjunto de dados.
#A cada linha do arquivo de entrada, o mapper extrai o peso, o id e o nome e adiciona uma tupla (weight, name) à lista top.
#A seguir, se o tamanho da lista top for maior do que 5, a lista é ordenada em ordem crescente com base nos pesos e o menor peso é removido usando o método pop(0). Isso garante que a lista top sempre contenha as cinco maiores pesos encontrados até o momento.
#Assim, no final do job, a lista top conterá as cinco maiores tuplas (weight, name) encontradas no conjunto de dados.
            
    def reducer_init(self):
        for item in self.top:
            yield(None, item)
#No caso deste código, o reducer_init itera sobre a lista self.top, que contém as cinco maiores tuplas (peso, nome) até o momento, e emite uma tupla vazia com chave None e valor (peso, nome) para cada uma delas.
#O None é utilizado como chave porque o reducer_init não realiza nenhuma agregação de valores, ele apenas emite as tuplas que foram previamente armazenadas na lista self.top. Portanto, não há necessidade de usar uma chave para agregação, e None é uma escolha conveniente para indicar que não há nenhuma chave associada aos valores emitidos.

                
    def reducer(self, key, values):
        items = list(values)
#A função reducer recebe uma chave (key) e uma lista de valores (values) como entrada. Neste código, os valores são convertidos em uma lista e atribuídos à variável items. Essa lista é então classificada em ordem decrescente (do maior para o menor) com base no primeiro elemento de cada tupla (peso).
            
        items.sort(reverse=True)
        for i in range(5):
            item = items[i]
            yield item[1], item[0]
#Essa parte se refere ao reducer, onde os itens armazenados no atributo top da classe Topn são recebidos como valores. Esses valores são convertidos em uma lista de tuplas denominada items (onde cada tupla contém um peso e um nome).
#Em seguida, a lista items é classificada em ordem decrescente com base no peso, usando o método sort(reverse=True). Depois disso, a lógica itera sobre os 5 primeiros elementos da lista classificada (os 5 itens mais pesados), e para cada um desses itens, é gerado um par chave-valor (no formato nome-peso) usando yield item[1], item[0]. O nome é a chave e o peso é o valor. Esses pares chave-valor são a saída do reducer e serão coletados e exibidos pelo MRJob.

    
if _name_ == '_main_':
    Topn.run()