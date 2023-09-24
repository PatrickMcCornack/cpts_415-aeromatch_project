# -*- coding: utf-8 -*-
"""
This code opens a connection to the Neo4j Aeromatch database and queries it to 
the find the shortest paths between 250 pre-set pairs of airports. The set of 
all pairs and the time taken to query them is then output in the form of a 
dataframe as a .csv file. 

Code adapted from query written by Kameron Galm. 
"""

#%%
# Set-up ----------------------------------------------------------------------
from neo4j import GraphDatabase
import time
import pandas as pd


graphdb = GraphDatabase.driver(uri = "bolt://localhost:7687", auth=("neo4j", "password"))

session = graphdb.session()

pairs = pd.read_csv('./airportID_pairs.csv')
src = pairs['src']
dst = pairs['dst']


#%% Query ---------------------------------------------------------------------
times = []

for i in range(0, len(src)):
    start_time = time.time()
    q1 = "MATCH (n:airport{airportID:'" + str(src[i]) + "'}), (n1:airport{airportID:'" + str(dst[i]) + "'}), p = shortestPath((n)-[*]-(n1)) RETURN p"
    nodes = session.run(q1)
    times.append(time.time() - start_time)

results = pd.DataFrame(list(zip(src, dst, times)), columns = ["src", "dst", "times"])
results.to_csv('./neo4j_.csv', index = False)

