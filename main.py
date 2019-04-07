import time, json, constant, re, sys, collections
from mpi4py import MPI

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map               #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
global post_counter                  # number of posts in each grid cell. eg: {'A1': 200, 'A2': 320}
global hashtag_counter               # number of hashtags in each grid cell. eg: {'A1': {'obama': 20, 'haha': 1, 'lucky': 3}, 'A2': {'stupid': 2}}


def init():
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
    post_counter = collections.Counter()
    hashtag_counter = {}


def load_map(filename = constant.MELB_GRID):
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map
    global post_counter
    global hashtag_counter
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
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
            coordinates_map[p['id']] = [xmin, xmax, ymin, ymax]
            post_counter[p['id']] = 0
            hashtag_counter[p['id']] = collections.Counter()


def broadcast_global(comm):
    comm.bcast(min_x, root=0)
    comm.bcast(max_x, root=0)
    comm.bcast(min_y, root=0)
    comm.bcast(max_y, root=0)
    comm.bcast(scale_x, root=0)
    comm.bcast(scale_y, root=0)
    comm.bcast(coordinates_map, root=0)
    comm.bcast(post_counter, root=0)
    comm.bcast(hashtag_counter, root=0)


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


def get_hashtags(text):
    words = text.split(' ')
    hashtags = []
    for word in words:
        if len(word) > 1 and word[0] == '#':
            hashtags.append(word.lower())
    return hashtags


def gen_results():
    for grid_cell, post_number in post_counter.most_common():
        print('{}: {} posts.'.format(grid_cell, post_number))
    for grid_cell, post_number in post_counter.most_common():
        print(grid_cell + ': ' + str(hashtag_counter[grid_cell].most_common(5)))


start = time.time()

comm = MPI.COMM_WORLD
sys.stdout = open("output_"+str(comm.rank)+".txt", "w", encoding='utf8')

init()
if comm.rank == 0:
    load_map()
broadcast_global(comm)

comm.Barrier()

with open(constant.SMALL_TWITTER, 'r', encoding='UTF-8') as input_file:
    for i, line in enumerate(input_file):
        if i % comm.size == comm.rank and re.search('{"id"', line):
            print('read line: '+str(i))
            coordinates = re.search('"coordinates":\[\d+\.*\d*,-\d+\.*\d*\]}', line)
            text = re.search('"text":', line)
            location = re.search(',"location":', line)
            if coordinates and text and location:
                coordinates_data = json.loads('{' + coordinates.group())['coordinates']
                area = locate(coordinates_data[0], coordinates_data[1])
                if area is not None:
                    post_counter[area] += 1
                    text = line[text.start()+8: location.start()-1]
                    tags = get_hashtags(text)
                    for tag in tags:
                        hashtag_counter[area][tag] += 1
comm.Barrier()

if comm.rank is 0:
    gen_results()

end = time.time()
print("Running time="+str(end-start))
