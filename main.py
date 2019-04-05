import ijson, json, constant, collections,re

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map                         #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
global counter
rigions = {}

def load_map(filename = constant.MELB_GRID):
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
        global min_x, max_x, min_y, max_y
        global scale_x, scale_y
        global coordinates_map
        global regions

        min_x = 180
        max_x = 0
        min_y = 0
        max_y = -90
        scale_x = None
        scale_y = None
        coordinates_map = {}
        for feature in data['features']:
            rigions[feature['properties']['id']] = collections.Counter()#build a seperate Counter for each rigion
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

def locate(coordinate):
    x, y = coordinate
    if min_x <= x < max_x and min_y < y <= max_y:
        i = int((max_y-y)/scale_y)
        if 0 <= i < len(constant.ALPHABET):
            first = constant.ALPHABET[i]
            second = int((x-min_x)/scale_x + 1)
            grid_cell = first+str(second)

            if grid_cell in coordinates_map:
                return grid_cell
    return None

def load_dataset(filename = constant.TINY_TWITTER):
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
        return data['rows']

def get_coordinate(twitter):
    return  twitter['value']['geometry']['coordinates']

def get_text(twitter):
    return twitter['value']['properties']['text']

def get_tags(text):
    return re.findall(' \#[^# ]* ',text)


load_map()
counter = collections.Counter()
for twitter in load_dataset():
    counter.update([locate(get_coordinate(twitter))])
    if locate(get_coordinate(twitter)) is not None:
        rigions[locate(get_coordinate(twitter))].update(get_tags(get_text(twitter)))

print(counter)
for area in rigions.keys():
    print('Top 5 of ',area,' are: ',rigions[area].most_common(5))
