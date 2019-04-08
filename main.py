import time, json, constant, re, sys, collections
from mpi4py import MPI

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map               #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
# global post_counter                  # number of posts in each grid cell. eg: {'A1': 200, 'A2': 320}
# global hashtag_counter               # number of hashtags in each grid cell. eg: {'A1': {'obama': 20, 'haha': 1, 'lucky': 3}, 'A2': {'stupid': 2}}


def init():
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map
    # global post_counter
    # global hashtag_counter
    min_x = 180
    max_x = 0
    min_y = 0
    max_y = -90
    scale_x = None
    scale_y = None
    coordinates_map = {}
    post_counter = collections.Counter()
    hashtag_counter = {}
    return [post_counter,hashtag_counter]


def load_map(post_counter,hashtag_counter,filename = constant.MELB_GRID):
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map
    # global post_counter
    # global hashtag_counter
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
            return [post_counter,hashtag_counter]


def broadcast_global(post_counter,hashtag_counter，comm):
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map
    # global post_counter
    # global hashtag_counter
    min_x = comm.bcast(min_x, root=0)
    max_x = comm.bcast(max_x, root=0)
    min_y = comm.bcast(min_y, root=0)
    max_y = comm.bcast(max_y, root=0)
    scale_x = comm.bcast(scale_x, root=0)
    scale_y = comm.bcast(scale_y, root=0)
    coordinates_map = comm.bcast(coordinates_map, root=0)
    post_counter = comm.bcast(post_counter, root=0)
    hashtag_counter = comm.bcast(hashtag_counter, root=0)
    returen [post_counter,hashtag_counter]


def locate(coordinate):
    x, y = coordinate
    dy = int((max_y-y)/scale_y)
    if float(dy) == (max_y-y)/scale_y:
        dy -= 1

    dx = int((x-min_x)/scale_x + 1)
    if float(dx) == (max_y-y)/scale_y:
        dx -= 1

    if dy in [0,1,2]:
        if dx < 1:
            dx = 1
    elif dy == 3:
        if dx < 3:
            dx = 3

    if dx in [0,1,2,3]:
        if dy < 0:
            dy = 0
    elif dx == 4:
        if dy > 2:
            dy = 2

    first = constant.ALPHABET[dy]
    grid_cell = first+str(dx)
    return grid_cell



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

post_counter,hashtag_counter = init()
if comm.rank == 0:
    post_counter,hashtag_counter = load_map(post_counter,hashtag_counter)
post_counter,hashtag_counter = broadcast_global(post_counter,hashtag_counter,comm)

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
comm.send(post_counter, dest=0)
comm.send(hashtag_counter, dest=0)

#comm.barrier()
if comm.rank == 0:
    #TODO: while closed_workers < num_workers: receive data and gather results
    src = MPI.ANY_SOURCE
    posts = comm.recv(source=src)
    hashtags = comm.recv(source=src)
    # calculate results


if comm.rank is 0:
    gen_results()

end = time.time()
print("Running time="+str(end-start))
