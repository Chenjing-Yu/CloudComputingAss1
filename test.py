import json, constant
global min_x, max_x, min_y, max_y
global scale_x, scale_y
global coordinates_map


def load_map(filename=constant.MELB_GRID):
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map
    min_x = 180
    max_x = 0
    min_y = 0
    max_y = -90
    scale_x = None
    scale_y = None
    coordinates_map = {}
    with open(filename, 'rb') as file:
        data = json.load(file)
        for feature in data['features']:
            p = feature['properties']
            xmin = p['xmin']
            xmax = p['xmax']
            ymin = p['ymin']
            ymax = p['ymax']
            min_x = min(min_x, xmin)
            max_x = max(max_x, xmax)
            min_y = min(min_y, ymin)
            max_y = max(max_y, ymax)
            if scale_x is None and scale_y is None:
                scale_x = round(xmax-xmin, 2)
                scale_y = round(ymax-ymin, 2)
            #coordinates_map[p['id']] = [xmin, xmax, ymin, ymax]
            if (xmin, xmax) in coordinates_map:
                coordinates_map[(xmin, xmax)][(ymin, ymax)] = p['id']
            else:
                coordinates_map[(xmin, xmax)] = {(ymin, ymax): p['id']}


def locate1(coordinates):
    x, y = coordinates
    if min_x <= x <= max_x and min_y <= y <= max_y:
        i = max(0, int(round((max_y-min_y), 2)/scale_y)-int(round((y-min_y), 2)/scale_y)-1)
        if i < len(constant.ALPHABET):
            first = constant.ALPHABET[i]
            second = max(1, int(round((max_x-min_x), 2)/scale_x)-int(round((max_x-x), 2)/scale_x))
            grid_cell = first+str(second)
            if grid_cell == 'D2':
                grid_cell = 'D3'
            if grid_cell == 'B5':
                grid_cell = 'C5'
            if grid_cell in coordinates_map:
                return grid_cell
    return None


def locate(coordinates):
    x, y = coordinates
    if min_x <= x <= max_x and min_y <= y <= max_y:
        for xrange, ymap in coordinates_map.items():
            if xrange[0] < x <= xrange[1]:
                for yrange, area in ymap.items():
                    if yrange[0] < y <= yrange[1]:
                        return area

    return None


load_map()
print(min_x, max_x, min_y, max_y)
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