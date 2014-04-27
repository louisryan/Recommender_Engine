###Recommendation Engine
Recommendation engines are key to some of the most iconic businesses on the web—think Amazon’s “Customers Who Bought This” section or Netflix’s personalized movie suggestions. Recommender systems take data collected on existing user behaviour and uses it to determine what users might also like. By using past behaviour of a large customer base, we can predict the taste preference of individuals.

This project aims to implement a scalable item to item recommender using Apache Hadoop, Mahout, Pig and Python. The reason why Hadoop is the tool of choice to implement the recommender is simple - On an e-commerce website, there could be 10,000 products for sale. Since a recommendation engine is computing correlations between pairs of items, the complexity is O(n x n), meaning that there could be up to 100,000,000 correlation computations. Since correlation calculations lend itself well to the MapReduce paradigm, we will use Hadoop. 

----------------------------
####Current Progress

1. Import purchase data into Apache Hadoop environment using Apache Pig.

2. Convert raw data in the form:

```
{"movie_id": "cffef2de02604b9b86ef36f81a91e583", "row_id": 0, "user_id": "c93e6253d45b42e6b8758c6078a20fdf", "purchase_price": 15, "movie_name": "the graduate"}
```

into the format that Apache Mahout accepts:

```
{"user_id": "c93e6253d45b42e6b8758c6078a20fdf", "movie_name": "the graduate", "1.0"}
```

3. Build a weighted graph of item-item links from a collection of user-item signals.