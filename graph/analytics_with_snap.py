import snap

from csv import reader


# G1 = snap.TUNGraph.New()

# max_row = False
# current_row = 0

# with open('processed_set_edges.csv', 'r') as read_obj:
#     csv_reader = reader(read_obj)
#     for row in csv_reader:

#         if max_row and current_row > max_row:
#             break

#         from_node = int(row[0])
#         to_node = int(row[1])

#         if not G1.IsNode(from_node):
#           G1.AddNode(from_node)
        
#         if not G1.IsNode(to_node):
#           G1.AddNode(to_node)

#         G1.AddEdge(from_node, to_node)

#         current_row = current_row + 1

# print("Done loading graph")
# modularity, CmtyV = G1.CommunityGirvanNewman()
# for Cmty in CmtyV:
#     print("Community: ")
#     for NI in Cmty:
#         print(NI)
# print("The modularity of the network is %f" % modularity)



# DegToCntV = G1.GetDegCnt()
# for item in DegToCntV:
#     # pass
#     print("%d nodes with degree %d" % (item.GetVal2(), item.GetVal1()))


# list_nodes_removed = []

# Components = G1.GetWccs()
# for i in range(0, len(Components)):
#     if i == 0:
#       print("Size of first component: %d" % Components[i].Len())
#       continue
    
#     for node in Components[i]:
#         list_nodes_removed.append(node)

# got = Components[0]

# remove node
# for node in list_nodes_removed:
# G1.DelNodes(list_nodes_removed)

# G1.PrintInfo()
# G1.SaveEdgeList('mygraph.txt')

# labels = {}
# for NI in G1.Nodes():
#     labels[NI.GetId()] = str(NI.GetId())
# G1.DrawGViz(snap.gvlDot, "output.png", " ", labels)


# print("start writing processed_set_edges.csv")
# with open('processed_set_edges.csv','a') as file:
#   with open('mygraph.txt', 'r') as read_obj:
#       i = 0
#       for row in read_obj:
#           if i == 0 or i == 1 or i == 2:
#               i = i+1
#               continue
#           row = row.strip('\n') 
#           pair_grs = row.split('\t')
#           int_pair_grs = [ int(i) for i in pair_grs]
#           file.write(f"{int_pair_grs[0]},{int_pair_grs[1]}\n")

#           i = i+1
# print("done writing processed_set_edges.csv")

