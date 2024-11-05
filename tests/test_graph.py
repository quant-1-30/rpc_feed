import networkx as nx
import matplotlib.pyplot as plt

# 创建一个有向无环图 (DAG)
G = nx.DiGraph()
G.add_edges_from([(5, 2), (5, 0), (4, 0), (4, 1), (2, 3), (3, 1)])

# 检查是否为 DAG
if nx.is_directed_acyclic_graph(G):
    # 获取拓扑排序
    topological_order = list(nx.topological_sort(G))
    print("Topological Sort (NetworkX):", topological_order)
else:
    print("The graph is not a DAG.")

# nx.draw(G, with_labels=True)
# plt.show()