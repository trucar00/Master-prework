import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Define latitude and longitude range
lat = 50
lon_min, lon_max = -10, 45

# Create map with PlateCarree projection
fig = plt.figure(figsize=(8, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

# Focus on Norway & surrounding north
ax.set_extent([-15, 50, 50, 88], crs=ccrs.PlateCarree())

# Add background features
ax.coastlines(resolution="50m")
ax.add_feature(cfeature.BORDERS, linestyle=":")
ax.add_feature(cfeature.LAND, facecolor="lightgray")
ax.add_feature(cfeature.OCEAN, facecolor="lightblue")
ax.gridlines(draw_labels=True, linestyle=":")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

# --- Plot segment of latitude line at 62°N ---

ax.plot([-10, -10, 45, 45], [88, 55, 55, 88], color="red", linewidth=1.2, label="Region")

# Optional: mark the endpoints

plt.ylabel("Latitude")
plt.xlabel("Longitude")
#plt.savefig("Figures/region2.eps")
plt.legend()
plt.tight_layout()
plt.gca().set_aspect('equal', adjustable='box')
plt.show()
