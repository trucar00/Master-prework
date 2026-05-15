import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import LineString
import contextily as ctx
import numpy as np
from pyproj import Transformer

# STYLE OF PLOT
plt.rcParams.update({
    "font.family": "serif",
    "mathtext.fontset": "cm",   # Computer Modern look
    "font.size": 16,
    "axes.labelsize": 16,
    "legend.fontsize": 14,
    "legend.title_fontsize": 16,
    "xtick.labelsize": 14,
    "ytick.labelsize": 14,
    "axes.axisbelow": True,
})

# Region limits
lat_min, lat_max = 55, 80
lon_min, lon_max = -10, 45

# Create ONE connected linestring
roi = LineString([
    (lon_min, lat_max),   # top-left
    (lon_min, lat_min),   # bottom-left
    (lon_max, lat_min),   # bottom-right
    (lon_max, lat_max)    # top-right
])

gdf = gpd.GeoDataFrame(geometry=[roi], crs="EPSG:4326")

# Convert to Web Mercator
gdf_web = gdf.to_crs(epsg=3857)

fig, ax = plt.subplots(figsize=(8, 8))

# Plot region outline
gdf_web.plot(
    ax=ax,
    color="red",
    linestyle="--",
    linewidth=2.2,
    zorder=3,
)

# Map extent
xmin, ymin, xmax, ymax = gdf_web.total_bounds
pad_x = (xmax - xmin) * 0.1
pad_y = (ymax - ymin) * 0.1

ax.set_xlim(xmin - pad_x, xmax + pad_x)
ax.set_ylim(ymin - pad_y, ymax)

# Add basemap
ctx.add_basemap(
    ax,
    source=ctx.providers.CartoDB.Positron,
    zoom=4
)

# ---- LAT/LON TICKS ----

transformer = Transformer.from_crs(
    "EPSG:4326",
    "EPSG:3857",
    always_xy=True
)

# Longitude ticks
xticks_lon = np.arange(lon_min, lon_max + 1, 10)
xticks_merc = [
    transformer.transform(lon, lat_min)[0]
    for lon in xticks_lon
]

# Latitude ticks
yticks_lat = np.arange(lat_min, lat_max + 1, 5)
yticks_merc = [
    transformer.transform(lon_min, lat)[1]
    for lat in yticks_lat
]

ax.set_xticks(xticks_merc)
ax.set_yticks(yticks_merc)

ax.set_xticklabels([f"{lon}°E" if lon >= 0 else f"{abs(lon)}°W"
                    for lon in xticks_lon])

ax.set_yticklabels([f"{lat}°N" for lat in yticks_lat])
ax.grid(linestyle="--")

plt.tight_layout()
plt.margins(x=0.02)
#plt.show()
plt.savefig("roi.pdf", bbox_inches="tight")