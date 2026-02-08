import matplotlib.pyplot as plt
import numpy as np
from sklearn.neighbors import BallTree


x = np.random.randint(0, 10, 5)
y = np.random.randint(0, 10, 5)

points = np.column_stack((x, y))

# Build Ball Tree
tree = BallTree(points, leaf_size=2)

# Query point
query = np.array([[5, 5]])
dist, ind = tree.query(query, k=4)

neighbors = points[ind[0]]

# Plot
plt.figure(figsize=(8,6))
plt.scatter(points[:,0], points[:,1], label="Data Points")
plt.scatter(query[:,0], query[:,1], c='red', s=120, label="Query Point")
plt.scatter(neighbors[:,0], neighbors[:,1], 
            facecolors='none', edgecolors='green', s=200, label="Nearest Neighbors")

for i,p in enumerate(neighbors):
    plt.plot([query[0,0], p[0]], [query[0,1], p[1]], '--')

plt.grid()
plt.legend()
plt.title("Ball Tree Nearest Neighbor Search")
plt.show()