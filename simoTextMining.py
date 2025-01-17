from pyspark import SparkContext
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.linalg import SparseVector
from pyspark.ml.linalg import DenseVector
from pyspark.sql import Row
from functools import partial
from pyspark.ml.regression import LinearRegression
import HTMLParser
import re, collections

#remove symbols and numbers and convert to lower case
def words(s):
	h = HTMLParser.HTMLParser()
	s=h.unescape(s)
	s=re.sub('[?!\\.,\(\)#\"\']+',' ',s).strip()
	s=re.sub('[ ]+[0-9]+[/]+[0-9]+[ ]+',' ',s)
	s=re.sub('[ ]+[0-9]+[\-]+[0-9]+[ ]+',' ',s)
	s=re.sub('[ ]+[0-9]+[\.]+[0-9]+[ ]+',' ',s)
	s=re.sub('[ ]+[0-9]+[,]+[0-9]+[ ]+',' ',s)
	s=re.sub('[\-]+',' ',s)
	s=re.sub('[0-9]+',' ',s)
	s=re.sub('([a-z]+)([A-Z]{1,1})([a-z]+)',r'\1 \2\3',s)
	s=re.sub('\s+',' ',s)
	return re.findall('[a-z]+', s.lower())
	


def fixEncoding(x):
    # fix encoding in fields name and value
    id = x['product_uid']
    name = ''
    if x['name'] is not None:
        name = x['name'].encode("UTF-8")
    value = ""
    if x['value'] is not None:
        value = x['value'].encode("UTF-8")
    retVal = '%s %s.' % (name, value)
    # return tuple instead of row
    return (id, [retVal])


def addFeatureLen(row):
    vector = row['tf_idf']
    size = vector.size
    newVector = {}
    for i, v in enumerate(vector.indices):
        newVector[v] = vector.values[i]
    newVector[size] = len(vector.indices)
    size += 1
    # we cannot change the input Row so we need to create a new one
    data = row.asDict()
    data['tf_idf'] = SparseVector(size, newVector)
    # new Row object with specified NEW fields
    newRow = Row(*data.keys())
    # fill in the values for the fields
    newRow = newRow(*data.values())
    return newRow


def cleanData(row, model):
    # we are going to fix search term field
    text = row['search_term'].split()
    for i, v in enumerate(text):
        text[i] = correct(v, model)
    data = row.asDict()
    # create new field for cleaned version
    data['search_term2'] = text
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow


def newFeatures(row):
    vector = row['tf_idf']
    data = row.asDict()
    data['features'] = DenseVector([len(vector.indices), vector.values.min(), vector.values.max()])
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow


def tfIdfAsNewFeatures(row):
    vector = row['tf_idf']
    data = row.asDict()    
    data['features'] = DenseVector([len(vector.indices), vector.values.min(), vector.values.max(), vector.values.mean()])
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow

def tfIdfAsNewFeaturesBis(row):
    vector = row['tf_idf']
    data = row.asDict()    
    data['features'] = DenseVector(vector.toArray())
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow

def enlargeToken(row):
    vectorT = row['words_title']
    vectorD = row['words_desc']
    data = row.asDict()
    data['words'] = vectorT + vectorD
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow

def enlargeTokenAndClean(row):
    vectorT = row['words_title']
    vectorD = row['words_desc']
    data = row.asDict()
    data['words'] = vectorT + vectorD
    w=[]
    for word in data['words']:
        w += words(word)
    data['wordsF'] = w
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow


sc = SparkContext.getOrCreate()

sqlContext = HiveContext(sc)
print "###############"
# READ data
data = sqlContext.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("inferSchema", "true").\
    load("train.csv").repartition(100)
print "data loaded - head:"
print data.head()
print "################"

attributes = sqlContext.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("inferSchema", "true").\
    load("attributes.csv").repartition(100)

print "attributes loaded - head:"
print attributes.head()
print "################"

product_description = sqlContext.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("inferSchema", "true").\
    load("product_descriptions.csv").repartition(100)

print "description loaded - head:"
print product_description.head()
print "################"


# attributes: 0-N lines per product
# Step 1 : fix encoding and get data as an RDD (id,"<attribute name> <value>")
attRDD = attributes.rdd.map(fixEncoding)
print "new RDD:"
print attRDD.first()
print "################"

# Step 2 : group attributes by product id
attAG = attRDD.reduceByKey(
    lambda x, y: x + y).map(lambda x: (x[0], ' '.join(x[1])))
print "Aggregated by product_id:"
print attAG.first()
print "################"

# Step 3 create new dataframe from aggregated attributes
atrDF = sqlContext.createDataFrame(attAG, ["product_uid", "attributes"])
print "New dataframe from aggregated attributes:"
print atrDF.head()
print "################"

# Step 4 join data with attribute

withAttdata = data.join(atrDF, ['product_uid'], 'left_outer')
print "Joined Data:"
print withAttdata.head()
#Row(product_uid=100501, id=2847, product_title=u'Ring Wireless Video Door Bell', search_term=u'door bell', relevance=3.0, attributes=u"Adjustable Volume Yes. Bullet04 Multiple faceplate finishes helping you match your current door hardware. Mechanical Bell No. Product Width (in.) 2.4. Bullet03 Built-in motion sensors detect movement up to 30 ft. allowing you to know what is going on outside of your home. Multiple Songs No. Digital Bell No. Product Height (in.) 5. Bullet02 Compatible with all iOS and android Smartphone and tablets. Door Chime Kit Type Wired With Contacts. Number of Sounds 1. Zone-specific Sounds No. Bell Button Color Family Gray. Certifications and Listings No Certifications or Listings. Product Depth (in.) .9. Electrical Product Type Door Chime Kit. Transformer Not Included. Bullet01 See and speak with visitors using your Smartphone or tablet, whether you're upstairs or across town. Door Bell Or Intercom Type Door Bells. Number of Buttons Included 2. Westminster Bell No. Bell Wire Required Wireless. Bullet05 Connect to current doorbell wiring or utilize internal battery for convenience. MFG Brand Name Ring. Style Contemporary.")
print "################"

# Step 5 join data with description
print "new RDD:"
print product_description.first()
print "################"

fulldata = withAttdata.join(product_description, ['product_uid'], 'left_outer')
print "Joined Data:"
print fulldata.head()
print "################"


# TF-IDF features
# Step 1: split text field into words
tokenizer = Tokenizer(inputCol="product_title", outputCol="words_title")
fulldata = tokenizer.transform(fulldata)
print "Tokenized Title:"
print fulldata.head()
print "################"

# Step 1 Prim: split text field into words
tokenizer = Tokenizer(inputCol="product_description", outputCol="words_desc")
fulldata = tokenizer.transform(fulldata)
print "Tokenized Description:"
print fulldata.head()
print "################"

#Merge product with words

fulldata = sqlContext.createDataFrame(fulldata.rdd.map((enlargeTokenAndClean)))                      
print "words enlarge with desc and title"
print fulldata.head()
print "################"                                    

# Step 2: compute term frequencies
hashingTF = HashingTF(inputCol="wordsF", outputCol="tf")
fulldata = hashingTF.transform(fulldata)
print "TERM frequencies:"
print fulldata.head()
print "################"
# Step 3: compute inverse document frequencies
idf = IDF(inputCol="tf", outputCol="tf_idf")
idfModel = idf.fit(fulldata)
fulldata = idfModel.transform(fulldata)
print "IDF :"
print fulldata.head()
print "################"

# Step 4 new features column / rename old
fulldata = sqlContext.createDataFrame(fulldata.rdd.map(addFeatureLen))
fulldata = sqlContext.createDataFrame(fulldata.rdd.map(newFeatures))
print "NEW features column :"
print fulldata.head()
print "################"


# Step 5: ALTERNATIVE ->ADD column with number of terms as another feature
#fulldata = sqlContext.createDataFrame(fulldata.rdd.map(
 #   addFeatureLen))  # add an extra column to tf features
#fulldata = fulldata.withColumnRenamed('tf_idf', 'tf_idf_plus')
#print "ADDED a column and renamed :"
#print fulldata.head()
#print "################"


# create NEW features & train and evaluate regression model
# Step 1: create features
fulldata = fulldata.withColumnRenamed(
    'relevance', 'label').select(['label', 'features'])
print "TRAIN - ADDED a column and renamed :"
print fulldata.head()
print "################"


# Simple evaluation : train and test split
(train, test) = fulldata.rdd.randomSplit([0.8, 0.2])

# Initialize regresion model
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(sqlContext.createDataFrame(train))

# Apply model to test data
result = lrModel.transform(sqlContext.createDataFrame(test))
# Compute mean squared error metric
MSE = result.rdd.map(lambda r: (r['label'] - r['prediction'])**2).mean()
print("Mean Squared Error = " + str(MSE))