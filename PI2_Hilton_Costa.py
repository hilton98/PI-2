import findspark
findspark.init()

from pyspark.sql import SparkSession

from pyspark import SparkConf, SparkContext 
conf = SparkConf().setMaster('local').setAppName("wc")
conf.set('spark.executor.memory','5G')
sc = SparkContext(conf = conf)

rdd_amazon = sc.textFile("amazon-meta.txt")
asin_prod = "1559362022"                      #produto de id=15, dado como exemplo


# (a) Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação

cutomers = []                                                           #comentarios do produto
rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line)

ASINS = rdd_ASIN.map(lambda line: line.split()[1])
ASIN_list = ASINS.collect()

rdd_reviews = rdd_amazon.filter(lambda line: "reviews: " in line)
reviews_prod = rdd_reviews.collect()

rdd_comentarios = rdd_amazon.filter(lambda line: "rating: " in line) 	  #comentarios referente a determinado produto
target_com = rdd_comentarios.collect()										              #[ASIN_list.index(asin_prod)-1]: posicao do comentario

conteudo = reviews_prod[ASIN_list.index(asin_prod)-1]						        #review do produto de pesquisa
i_target = target_com.index(conteudo)										                # " cabeçalho " do comentario em questao
total = int(target_com[i_target].split()[2])								            #quantidade de comentarios feitos

for i in range(total):                                                  #coleta dos comentarios
	cutomers.append(target_com[i_target+1+i])

rdd_h = sc.parallelize(cutomers)                                        #rdd dos comentarios do produto
resultado = rdd_h.collect()                                             #comentarios do produto
comentarios = []                                                        #formato do resultado final
for i in range( len( resultado ) ):
  comentarios.append( (resultado[i].split()[0], resultado[i].split()[2], int(resultado[i].split()[4]), int(resultado[i].split()[8]) ) )

ranking = sc.parallelize(comentarios )                                  #rdd formatado com data,cliente,rating,helpful
print("# 5 comentários mais úteis e com maior avaliação")
print(ranking.takeOrdered(5, lambda line: -1 * line[2] ) )

print("# 5 comentários mais úteis e com menor avaliação")
print(ranking.takeOrdered(5, lambda line: -1 * line[3] ) )



# (b) Dado um produto, listar os produtos similares com maiores vendas do que ele
'''
pos_similares = []                                                     #posição de cada produto
maior_venda = []

rdd_sales = rdd_amazon.filter(lambda line: "salesrank: " in line)
sales_list = rdd_sales.collect()                                       #lista de salesrank

rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line)
ASINS = rdd_ASIN.map(lambda line: line.split()[1])
ASIN_list = ASINS.collect()                                            #lista de asins
pos = ASIN_list.index(asin_prod)-1                                     #posicao do produto procurado

rdd_similar = rdd_amazon.filter(lambda line:"similar: " in line)
similar_list = rdd_similar.collect()                                   #lista de similares de todos os produtos

prod_sim = similar_list[pos].split("similar: ")[1]                     #tudo da lista similar exceto "similar: "
prod_similares = prod_sim.split()                                      #todos os similares ao produto alvo
#print(prod_similares)

for i in range(len(prod_similares)-1):
  pos_similares.append(ASIN_list.index(prod_similares[i+1]) )          #localização dos produtos

for i in range(len(pos_similares)):
  if(int(sales_list[pos_similares[i]].split()[1]) < int(sales_list[pos].split()[1]) ):
    #print(ASIN_list[pos_similares[i+1]], sales_list[ pos_similares[i] ])
    maior_venda.append("ASIN: "+ASIN_list[pos_similares[i+1]] + sales_list[ pos_similares[i] ] )

print(maior_venda)
'''

#(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
'''
cutomers = [] #comentarios alvo
rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line)
rdd_ASIN = rdd_ASIN.map(lambda line: line.split()[1])
ASIN_list = rdd_ASIN.collect()                                    #cada linha consiste em um produto
comentario_prod = ASIN_list.index(asin_prod)                      #posição do comentario em rdd de reviews
rdd_reviews = rdd_amazon.filter(lambda line: "reviews: " in line) #filtragem de linhas com reviews
reviews_list = rdd_reviews.collect()                              #cada linha consiste em um produto
cabecalho_rev = reviews_list[comentario_prod-1]                   #cabecalho dos comentarios do produto
total_c = int(cabecalho_rev.split()[2])                           #numero total de comentarios
rdd_reviews = rdd_amazon.filter(lambda line: "rating: " in line)  #linhas pertencem ao mesmo produto
comentarios = rdd_reviews.collect()                               #todos os cabecalhos seguidos dos comentarios
cabecalho_pos = comentarios.index(cabecalho_rev)                  #posicao do cabecalho na lista de comentarios
for i in range(total_c):
  aux = comentarios[cabecalho_pos+1+i]
  cutomers.append( (aux.split("-")[0], int(aux.split()[4]) ) )
result_cutomers = sc.parallelize(cutomers)
result_soma = result_cutomers.reduceByKey(lambda x,y: x+y )
result_soma = result_soma.collect()                               #lista de todos os valores somado por chave
result_chave = result_cutomers.groupByKey()
result_chave = result_chave.collect()                             #lista de todos os valores por chave
result = []
for i in range(len(result_chave)):
  result.append( (result_soma[i][0], result_soma[i][1] / float(len( result_chave[i][1] ) ) ) )
print( result )
'''



#(d) Listar os 10 produtos lideres de venda em cada grupo de produtos

'''
lista_cat = []

rdd_grupos = rdd_amazon.filter(lambda line: " group: " in line)
rdd_sales = rdd_amazon.filter(lambda line: " salesrank: " in line)
rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line )

la = rdd_ASIN.collect()
lg = rdd_grupos.collect()
ls = rdd_sales.collect()

for i in range(len(lg) - 1):
  lista_cat.append( ( la[i+1],lg[i].split()[1], int( ls[i].split()[1] ) ) )

rdd_geral = sc.parallelize(lista_cat)

rdd_geral = rdd_geral.filter(lambda line: line[2] != -1 )

rddAux = rdd_geral.filter(lambda line: "Book" in line)
print(rddAux.takeOrdered(10, lambda line: line[2]) )


rddAux = rdd_geral.filter(lambda line: "Music" in line)
print(rddAux.takeOrdered(10, lambda line: line[2]) )

rddAux = rdd_geral.filter(lambda line: "DVD" in line)
print(rddAux.takeOrdered(10, lambda line: line[2]) )

rddAux = rdd_geral.filter(lambda line: "Video" in line)
print(rddAux.takeOrdered(10, lambda line: line[2]) )
'''


'''
#(e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto

avgs = []
rdd_grupos = rdd_amazon.filter(lambda line: " group: " in line)
rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line )
rdd_avg = rdd_amazon.filter(lambda line: "reviews: " in line)
#rdd_avg = rdd_avg.map(lambda line: int(line.split()[7] ) )

group_lista = rdd_grupos.collect()
ASIN_lista = rdd_ASIN.collect()
avg_lista = rdd_avg.collect()

for i in range( len(group_lista) ):
  avgs.append( ( group_lista[i], ASIN_lista[i+1] , float(avg_lista[i].split()[7]) ) )

avg = sc.parallelize(avgs, numSlices=20)

print("por Hilton Costa")
result = avg.filter(lambda line: "Book" in line[0])
print(result.takeOrdered(10, lambda line: -1 * line[2]))

result = avg.filter(lambda line: "Music" in line[0])
print(result.takeOrdered(10, lambda line: -1 * line[2]))

result = avg.filter(lambda line: "DVD" in line[0])
print(result.takeOrdered(10, lambda line: -1 * line[2]))

result = avg.filter(lambda line: "Video" in line[0])
print(result.takeOrdered(10, lambda line: -1 * line[2]))
'''



'''
---exemplo dado
Id:   15
ASIN: 1559362022
  title: Wake Up and Smell the Coffee
  group: Book
  salesrank: 518927
  similar: 5  1559360968  1559361247  1559360828  1559361018  0743214552
  categories: 3
   |Books[283155]|Subjects[1000]|Literature & Fiction[17]|Drama[2159]|United States[2160]
   |Books[283155]|Subjects[1000]|Arts & Photography[1]|Performing Arts[521000]|Theater[2154]    
reviews: total: 8  downloaded: 8  avg rating: 4
    2002-5-13  cutomer: A2IGOA66Y6O8TQ  rating: 5  votes:   3  helpful:   2
    2002-6-17  cutomer: A2OIN4AUH84KNE  rating: 5  votes:   2  helpful:   1
    2003-1-2   cutomer: A2HN382JNT1CIU  rating: 1  votes:   6  helpful:   1
    2003-6-27  cutomer: A39QMV9ZKRJXO5  rating: 4  votes:   1  helpful:   1
    2004-2-17  cutomer:  AUUVMSTQ1TXDI  rating: 1  votes:   2  helpful:   0
    2004-10-13 cutomer:  A5XYF0Z3UH4HB  rating: 5  votes:   1  helpful:   1
 '''
