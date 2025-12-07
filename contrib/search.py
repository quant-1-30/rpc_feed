def bfs_by_level(G, start_nodes):
    visited = set()  # To track visited nodes
    queue = deque([(start, 0) for start in start_nodes])  # Initialize queue with nodes and their level (0)
    for start in start_nodes:
        visited.add(start)  # Mark all start nodes as v
    
    levels = defaultdict(list)

    while queue:
        node, current_level = queue.popleft()

        # Add the node to the corresponding level in the levels dictionary
        if current_level not in levels:
            levels[current_level].append(node)

        # Enqueue all unvisited neighbors with the incremented level
        for neighbor in G.neighbors(node):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, current_level + 1))

    return levels

