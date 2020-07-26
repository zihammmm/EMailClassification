# EMailClassification
## TODO LIST
+ ~~搞个停词表 https://juejin.im/post/5eca1fab6fb9a04805261de6~~
+ 使用实验二中计算TF-IDF的算法来计算

## naive bayesian model
### 多项式模型
在多项式模型中，设某文档d=(t1,t2,…,tk)，tk是该文档中出现过的单词，允许重复，则
+ 先验概率P(c)= 类c下单词总数/整个训练样本的单词总数
  
+ 类条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/(类c下单词总数+|V|)
  
+ V是训练样本的单词表（即抽取单词，单词出现多次，只算一个），|V|则表示训练样本包含多少种单词。在这里，m=|V|, p=1/|V|。
P(tk|c)可以看作是单词tk在证明d属于类c上提供了多大的证据，而P(c)则可以认为是类别c在整体上占多大比例(有多大可能性)。

https://blog.csdn.net/hao5335156/article/details/82716923
https://www.cnblogs.com/kexinxin/p/10049910.html

https://tengzi-will.github.io/2018/12/24/Hadoop-实现朴素贝叶斯-Naive-Bayes-文本分类/

+ 类c下的单词总数   -> 相加可以得到整个训练样本的单词总数
+ 类c下的单词， 出现次数  -> 将单词数相加可以得到|v|