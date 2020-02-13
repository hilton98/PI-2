import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


conf = SparkConf().setMaster('local').setAppName("wc")
conf.set('spark.executor.memory','5G')
sc = SparkContext(conf = conf)

rdd_amazon = sc.textFile("amazon-meta.txt")
asin_prod = "0738700797"                      							#produto de id=15, dado como exemplo


# (a) Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação

	
cutomers = []                                                           #comentarios do produto
comentarios = []                                                        #formato do resultado final

rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line)
ASINS = rdd_ASIN.map(lambda line: line.split()[1])
ASIN_list = ASINS.collect()

rdd_reviews = rdd_amazon.filter(lambda line: "reviews: " in line)
reviews_prod = rdd_reviews.collect()

rdd_comentarios = rdd_amazon.filter(lambda line: "rating: " in line) 	#comentarios referente a determinado produto
target_com = rdd_comentarios.collect()									#[ASIN_list.index(asin_prod)-1]: posicao do comentario

conteudo = reviews_prod[ASIN_list.index(asin_prod)-1]					#review do produto de pesquisa
i_target = target_com.index(conteudo)									# " cabeçalho " do comentario em questao
total = int(target_com[i_target].split()[2])							#quantidade de comentarios feitos

for i in range(total):                                                  #coleta dos comentarios
	cutomers.append(target_com[i_target+1+i])
rdd_h = sc.parallelize(cutomers)                                        #rdd dos comentarios do produto
resultado = rdd_h.collect()                                             #comentarios do produto
for i in range( len( resultado ) ):
  comentarios.append( (resultado[i].split()[0], resultado[i].split()[2], int(resultado[i].split()[4]), int(resultado[i].split()[8]) ) )

spark = SparkSession.builder.appName("wc").config("spark.some.config.option", "some-value").getOrCreate()
sc = ("spark.SparkContext")
df = spark.createDataFrame(comentarios, ["Data","Cliente","Rating","Helpful"])
df.createOrReplaceTempView("Comentarios") #criação da view

#dataframe
print("\n 	operações com dataframes")
print("# 5 comentários mais úteis e com maior avaliação")
df.select("*").where("Rating = 5").orderBy("Helpful", ascending=False).show(5)

print("# 5 comentários mais úteis e com menor avaliação")
df.select("*").where("Rating < 5").orderBy("Helpful", ascending=False).show(5)


#sql

print("\n 	operações com SQL")
print("# 5 comentários mais úteis e com maior avaliação")
spark.sql("SELECT * FROM (SELECT * FROM Comentarios WHERE Rating = 5) ORDER BY Helpful DESC LIMIT 5").show()

print("# 5 comentários mais úteis e com menor avaliação")
spark.sql("SELECT * FROM (SELECT * FROM Comentarios WHERE Rating < 5) ORDER BY Helpful DESC LIMIT 5").show()



'''

# (b) Dado um produto, listar os produtos similares com maiores vendas do que ele

#dataframe com respectivo ASIN e salesrank

dfs = []	#data frame similares e respectivo asin
asin_sales = []

rdd_salesrank = rdd_amazon.filter(lambda line: "salesrank: " in line)
salesrank =  rdd_salesrank.collect()

rdd_asins = rdd_amazon.filter(lambda line: "ASIN: " in line)
asins=rdd_asins.collect()

for i in range(len(salesrank)):
	asin_sales.append( (asins[i+1].split()[1] , int(salesrank[i].split()[1]) ) )		#dados para o dataframe asin e salesrank


rdd_similar = rdd_amazon.filter(lambda line: "similar: " in line)
similar=rdd_similar.collect()

for i in range(len(similar)):
	for j in range( len(similar[i].split()) - 2 ):
		dfs.append( (asins[i+1].split()[1] , similar[i].split()[j+2] ) )				#asin e similares


#Fim parser/início consulta


spark = SparkSession.builder.appName("wc").config("spark.some.config.option", "some-value").getOrCreate()
sc = ("spark.SparkContext")

#dfs = [(1,2),(1,3),(5,2),(9,4)]
#asin_sales = [ (1, 200), (2,152), (3,201), (4,122), (5,1002), (9,50) ]

asin_sim = spark.createDataFrame(dfs,["ASIN","similar"])
asin_sal = spark.createDataFrame(asin_sales, ["ASIN_p","salesrank"] )

tsim = asin_sim.alias("tsim")
tsal = asin_sal.alias("tsal")


#dataframe
print("\n 	operações com dataframes")

a = tsal.join(tsim, tsim.ASIN == tsal.ASIN_p ).select(tsal.salesrank.alias("produto_cod"),"similar").where("ASIN_p = "+asin_prod+"")
b = a.join(tsal, tsal.ASIN_p == a.similar ).select("ASIN_p","salesrank").where("produto_cod > salesrank").show()

#sql
asin_sim.createOrReplaceTempView("a_sim")
asin_sal.createOrReplaceTempView("a_sal")

print("\n 	operações com SQL")
spark.sql(" SELECT a.ASIN_p, a.salesrank FROM a_sal a, ( SELECT similar,salesrank FROM a_sim,a_sal \
		WHERE ASIN = "+asin_prod+" AND ASIN_p = ASIN ) x \
		WHERE x.similar = a.ASIN_p AND a.salesrank < x.salesrank").show()

'''



#(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
'''
rdd_ASIN = rdd_amazon.filter(lambda line: "ASIN: " in line)
rdd_ASIN = rdd_ASIN.map(lambda line: line.split()[1])
rdd_reviews = rdd_amazon.filter(lambda line: " reviews: " in line)

asin = rdd_ASIN.collect()
reviews = rdd_reviews.collect()
pos = asin.index("B000007R0T")-1 #produto de ID 18
comentario = reviews[pos]

rdd_rating = rdd_amazon.filter(lambda line: "rating: " in line)
rating = rdd_rating.collect()

formato = []
for i in range( rating.index(comentario)+1 ,int(comentario.split()[2]) + rating.index(comentario) + 1 ):
	formato.append( (rating[i].split("-")[0] , int(rating[i].split()[4]) ) )


#fim parser/inicio consulta
spark = SparkSession.builder.appName("wc").config("spark.some.config.option", "some-value").getOrCreate()
sc = ("spark.SparkContext")

tableComentarios = spark.createDataFrame(formato, ["Data","Rating"] )
tableComentarios.createOrReplaceTempView("tcom")

#dataframe
print("\n 	operações com dataframes")
tableComentarios.groupBy("Data").avg("Rating").show()

#sql

print("\n 	Operações com SQL")
spark.sql("SELECT Data, AVG(Rating) FROM tcom GROUP BY Data").show()
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
  lista_cat.append( ( la[i+1].split()[1],lg[i].split()[1], int( ls[i].split()[1] ) ) )

#fim parser/inicio consulta

spark = SparkSession.builder.appName("wc").config("spark.some.config.option", "some-value").getOrCreate()
sc = ("spark.SparkContext")


tableProd = spark.createDataFrame(lista_cat, ["ASIN","Grupo","salesrank"])
tableProd.createOrReplaceTempView("table")

#dataframe
print("\n 	operações com dataframes")
tableProd.select("*").where("Grupo = 'Book' AND salesrank <> -1 ").orderBy("salesrank").show(10)
tableProd.select("*").where("Grupo = 'Music' AND salesrank <> -1 ").orderBy("salesrank").show(10)
tableProd.select("*").where("Grupo = 'DVD' AND salesrank <> -1 ").orderBy("salesrank").show(10)
tableProd.select("*").where("Grupo = 'Video' AND salesrank <> -1 ").orderBy("salesrank").show(10)

#sql
print("\n 	operações com SQL")
spark.sql("SELECT * FROM (SELECT * FROM table WHERE Grupo = 'Book' AND salesrank <> -1 ) ORDER BY salesrank LIMIT 10").show()
spark.sql("SELECT * FROM (SELECT * FROM table WHERE Grupo = 'Music' AND salesrank <> -1 ) ORDER BY salesrank LIMIT 10").show()
spark.sql("SELECT * FROM (SELECT * FROM table WHERE Grupo = 'DVD' AND salesrank <> -1 ) ORDER BY salesrank LIMIT 10").show()
spark.sql("SELECT * FROM (SELECT * FROM table WHERE Grupo = 'Video' AND salesrank <> -1 ) ORDER BY salesrank LIMIT 10").show()

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
  avgs.append( ( group_lista[i].split()[1], ASIN_lista[i+1].split()[1] , float(avg_lista[i].split()[7]) ) )


spark = SparkSession.builder.appName("wc").config("spark.some.config.option", "some-value").getOrCreate()
sc = ("spark.SparkContext")

formato = spark.createDataFrame(avgs, ["Grupo", "ASIN", "AVG_rat"])
formato.createOrReplaceTempView("formato")

#dataframe

print("\n 	operações com dataframes")
formato.select("*").where("Grupo = 'Book'").orderBy("AVG_rat", ascending=False).show(10)
formato.select("*").where("Grupo = 'Music'").orderBy("AVG_rat", ascending=False).show(10)
formato.select("*").where("Grupo = 'DVD'").orderBy("AVG_rat", ascending=False).show(10)
formato.select("*").where("Grupo = 'Video'").orderBy("AVG_rat", ascending=False).show(10)

#sql
print("\n 	operações com SQL")
spark.sql("SELECT * FROM (SELECT * FROM formato WHERE Grupo = 'Book') ORDER BY AVG_rat DESC LIMIT 10").show()
spark.sql("SELECT * FROM (SELECT * FROM formato WHERE Grupo = 'Music') ORDER BY AVG_rat DESC LIMIT 10").show()
spark.sql("SELECT * FROM (SELECT * FROM formato WHERE Grupo = 'DVD') ORDER BY AVG_rat DESC LIMIT 10").show()
spark.sql("SELECT * FROM (SELECT * FROM formato WHERE Grupo = 'Video') ORDER BY AVG_rat DESC LIMIT 10").show()

'''
