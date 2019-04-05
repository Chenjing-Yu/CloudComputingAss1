import time
import ijson
import json
import operator
import collections
import constant

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map               #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
global post_counter # number of posts in each grid cell. eg: {'A1': 200, 'A2': 320}
global hashtag_counter # number of hashtags in each grid cell. eg: {'A1': {'obama': 20, 'haha': 1, 'lucky': 3}, 'A2': {'stupid': 2}}

def load_map(filename = constant.MELB_GRID):
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
        global min_x, max_x, min_y, max_y
        global scale_x, scale_y
        global coordinates_map
        global post_counter
        global hashtag_counter
        min_x = 180
        max_x = 0
        min_y = 0
        max_y = -90
        scale_x = None
        scale_y = None
        coordinates_map = {}
        post_counter = {}
        hashtag_counter = {}
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
            post_counter[p['id']] = 0
            hashtag_counter[p['id']] = {}

def locate(x, y):
    if min_x <= x < max_x and min_y < y <= max_y:
        i = int((max_y-y)/scale_y)
        if 0 <= i < len(constant.ALPHABET):
            first = constant.ALPHABET[i]
            second = int((x-min_x)/scale_x + 1)
            grid_cell = first+str(second)
            if grid_cell in coordinates_map:
                return grid_cell
    return None

#extract needed tags, and output reduced file/data
def digest(filename, batch_size = 5000):
    with open(filename, 'rb') as input_file:
        #parser = ijson.parse(input_file)
        counter = 0
        output = {'rows': [], }
        objects = ijson.items(input_file, 'rows.item.value')
        for object in objects:
            output['rows'].append({'coordinates': [float(object['geometry']['coordinates'][0]),
                                                   float(object['geometry']['coordinates'][1])],
                                 'text': object['properties']['text']})
            counter += 1
        # with open('data.json', 'w') as outfile:
        #     json.dump(output, outfile)
        print("total post number:"+str(counter))
        return output

# def get_hashtags(text):
#     words = text.split(' ')
#     hashtags = []
#     for word in words:
#         if len(word) >1 and word[0] == '#':
#             hashtags.append(word[1:])
#     return hashtags

def count_hashtags(grid_cell, text):
    words = text.split(' ')
    for word in words:
        if len(word) >1 and word[0] == '#':
            word = word.lower()
            if word in hashtag_counter[grid_cell]:
                hashtag_counter[grid_cell][word] += 1
            else:
                hashtag_counter[grid_cell][word] = 1

#statistics
def do_statistics(data):
    for row in data['rows']:
        x = row['coordinates'][0]
        y = row['coordinates'][1]
        grid_cell = locate(x, y)
        if (grid_cell != None):
            post_counter[grid_cell] += 1
            count_hashtags(grid_cell, row['text'])

def gen_results():
    sorted_post = sorted(post_counter.items(), key=lambda kv: kv[1], reverse=True)
    for grid_cell, post_number in sorted_post:
        print('{}: {} posts.'.format(grid_cell, post_number))
    for grid_cell, post_number in sorted_post:
        sorted_hashtags = sorted(hashtag_counter[grid_cell].items(), key=lambda kv: kv[1], reverse=True)
        #hashtag_counter[grid_cell] = sorted_hashtags[:5]
        print(grid_cell + ': ' + str(sorted_hashtags[:5]))


start = time.time()

load_map()
print(coordinates_map)
print(min_x, max_x, min_y, max_y)
print(scale_x, scale_y)
grid_cell = locate(145.449, -38.0)
print(grid_cell)
#running time consuming: about 12s for smallTwitter, mainly on data reading and parsing, not writing files
reduced_data = digest(constant.SMALL_TWITTER)

do_statistics(reduced_data)
gen_results()

end = time.time()
print("Running time="+str(end-start))