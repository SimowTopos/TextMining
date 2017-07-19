**DSSP6 Text Mining project report**

**Project discovery**

We started the datacamp by analysing and understanding the code exemple.
For our first execution by consedering (the initial example given in the code) :
- the tf-idf of tokenized word title feature joined with the attribute feature
- the addition of the size vector as a new coordinate
- the training with the lenth and min of the enriched tf-idf vector
  - Exemple of one product : features=DenseVector([10.0, 3.6652])

By considering word title and attribute, we computed the Mean Squared Error = 0.284223501857

**First expirementation**

We tried with a 0 DenseVector :
  features=DenseVector([0.0, 0.0])
  We had this result :
  Mean Squared Error = 0.281658352253 
  Which is less than the first one. We didn't succeed to intreprete this result...It's probably due to some error in our code.
  
**New feature experimentaion**

We tried with the word description and attribute.
we computed the Mean Squared Error = 0.282793946361

We saw a small improvement due to probably the information added by the desciption data which is better than the title

**Mixing features**

We tried with the word title, word description and attribute data.
```python
def enlargeTokenAndClean(row):
    vectorT = row['words_title']
    vectorD = row['words_desc']
    data = row.asDict()
    data['words'] = vectorT + vectorD
    newRow = Row(*data.keys())
    newRow = newRow(*data.values())
    return newRow
    
    .......
    
    fulldata = sqlContext.createDataFrame(fulldata.rdd.map((enlargeTokenAndClean)))
    
    ....
    
    hashingTF = HashingTF(inputCol="words", outputCol="tf")
    
    ...
    
```

The result was : **Mean Squared Error = 0.280816931591**
We improved the score by this features engineering.

**Changing the structure of the feature**

We tried to add the mean value in the DenseVector. This lead to a worst score :
Mean Squared Error = 0.283601709652
It wasn't a good idea as we add some noise to the feature...
So we remove it for the next steps.

**Cleanning data**

We tried to remove symbols and numbers and convert to lower case by using the **words** function.
The result was very surprising and we need to analyse why :
```python
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
```

The result was : Mean Squared Error = 0.286859147648, wish is worst....We probably miss something

**Conclusion**
Our best score : **Mean Squared Error = 0.280816931591**

We need to add some tuning as proposed in the datacamp description...To be continued
