# get all entity mapping address
curl 
unzip 

# get all block from 1 to 550000
./blocks/get_blocks.py -s 1 -e 550000

# get all transaction and inoutflow from start block to end block -> save to collections famous_address_txns_v2, famous_inoutflow_v2 
./address/process_txns_for_famous_address.py -s 1 -e 550000

# extract graph from start block to end block -> save edges to collections edges_grouping_graph_v2
# then it will do some preprocessing for the graph, and output the final graph in processed_set_edges.csv
./graph/extract_graph_new_strategy.py -s 1 -e 550000 

