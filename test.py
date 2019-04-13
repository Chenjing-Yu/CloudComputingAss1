import json, constant
global coordinates_map


def load_map(filename=constant.MELB_GRID):
    global coordinates_map
    coordinates_map = []
    with open(filename, 'r') as file:
        data = json.load(file)
        for feature in data['features']:
            coordinates_map.append(feature['properties'])


def locate(coordinates):
    x, y = coordinates
    for item in coordinates_map:
        if item['xmin'] <= x <= item['xmax'] and item['ymin'] <= y <= item['ymax']:
            return item['id']
    return None


load_map()
a1_1 = [144.7, -37.5]
a1_2 = [144.85, -37.65]
a1_3 = [144.85, -37.5]
a1_4 = [144.7, -37.65]
print(locate(a1_1))
print(locate(a1_2))
print(locate(a1_3))
print(locate(a1_4))
a2_1 = [145, -37.5]
a2_2 = [145, -37.65]
print(locate(a2_1))
print(locate(a2_2))
b1_1 = [144.7, -37.8]
b1_2 = [144.85, -37.8]
print(locate(b1_1))
print(locate(b1_2))
b2_1 = [145, -37.8]
print(locate(b2_1))
d3_1 = [145, -38.1]
d3_2 = [145.15, -38.1]
print(locate(d3_1))
print(locate(d3_2))
c5_1 = [145.45, -37.95]
c5_2 = [145.45, -37.8]
print(locate(c5_1))
print(locate(c5_2))