import snap
import networkx as nx
from csv import reader
import matplotlib.pyplot as plt

G = nx.Graph()

max_row = False
current_row = 0

with open('set_edges.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    for row in csv_reader:

        if max_row and current_row > max_row:
            break
        
        from_node = int(row[0])
        to_node = int(row[1])

        G.add_edge(from_node, to_node)

        current_row = current_row + 1

nx.draw(G)
ax = plt.gca()
ax.margins(0.20)
plt.axis("off")
# plt.show()
plt.savefig('foo.pdf')

