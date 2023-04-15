from mrjob.job import MRJob

class WeatherStats(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        if fields[2] == 'TMAX' or fields[2] == 'TMIN': #delimita as linhas da análise desconsiderando linhas dif de TMAX e TMIN
            yield fields[0], float(fields[3]) 
            #fields[0] define a coluna 1 => na tabela representa a localização
            #float(fields[3] define a coluna 2 => na tabela representa as temperaturas-função float converte para número
            #esta conversão é necessária para realizar as funções matematicas nos dados na etapa reducer

    def reducer(self, location, temps): #location => fields[0], temps => fields[3]
        temps_list = list(temps) #cria uma tupla com a relação das temperaturas
        max_temp = max(temps_list) #obtêm o valor máximo da lista
        min_temp = min(temps_list) #obtêm o valor mínimo da lista
        yield location, (max_temp, min_temp) #aglutina para cada location o max e min da lista temps(temperatura)

if __name__ == '__main__':
    WeatherStats.run()
#Primeiro, importamos a classe MRJob do módulo mrjob. Isso nos permite criar um job do MapReduce que pode ser executado em um cluster ou localmente.

#Em seguida, criamos uma classe WeatherStats que herda de MRJob. Essa classe define o job do MapReduce que estamos criando.

#Na função mapper, recebemos como entrada uma chave _ (que não é usada) e uma linha do arquivo de entrada. Usamos a função split para separar a linha em campos, que são armazenados em uma lista fields. O terceiro campo (fields[2]) contém a informação se a linha se refere a uma temperatura máxima ou mínima. Se for o caso, emitimos um par chave-valor com a data (segundo campo, fields[1]) e a temperatura (quarto campo, fields[3]). Note que convertemos a temperatura para um valor float.

#Na função reducer, recebemos como entrada uma data e uma lista de temperaturas. Convertemos a lista de temperaturas em uma lista Python (usando a função list()) e então usamos as funções max() e min() para encontrar a temperatura máxima e mínima na lista. Em seguida, emitimos um par chave-valor com a data e uma tupla contendo a temperatura máxima e mínima.

#Por fim, no bloco if _name_ == '_main_':, criamos uma instância da classe WeatherStats e chamamos o método run(). Isso executa o job do MapReduce localmente.

# command: $ python map_reduce_4.py 1800.csv >> 1800_result.csv