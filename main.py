import ijson
import json
import constant

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map                           #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]

def load_map(filename = constant.MELB_GRID):
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
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
            if scale_x == None and scale_y == None:
                scale_x = round(xmax-xmin, 2)
                scale_y = round(ymax-ymin, 2)
            coordinates_map[p['id']] = [xmin, xmax, ymin, ymax]

def locate(x, y):
    if min_x <= x < max_x and min_y < y <= max_y:
        i = int((max_y-y)/scale_y)
        print(i)
        if 0 <= i < len(constant.ALPHABET):
            first = constant.ALPHABET[i]
            second = int((x-min_x)/scale_x + 1)
            grid_cell = first+str(second)
            print(grid_cell)
            if grid_cell in coordinates_map:
                return grid_cell
    return None

load_map()
print(coordinates_map)
print(min_x, max_x, min_y, max_y)
print(scale_x, scale_y)
grid_cell = locate(145.449, -38.0)
print(grid_cell)

#TODO: parse twitter file, generate a intermediate output file?
global post_counter # number of posts in each grid cell. eg: {'A1': 200, 'A2': 320}
global hashtag_counter # number of hashtags in each grid cell. eg: {'A1': {'obama': 20, 'haha': 1, 'lucky': 3}, 'A2': {'stupid': 2}}
