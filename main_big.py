import time, json, constant, re, sys, collections
from mpi4py import MPI

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map               #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
global post_counter                  # number of posts in each grid cell. eg: {'A1': 200, 'A2': 320}
global hashtag_counter               # number of hashtags in each grid cell. eg: {'A1': {'obama': 20, 'haha': 1, 'lucky': 3}, 'A2': {'stupid': 2}}
global comm


def init():
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map, post_counter, hashtag_counter
    min_x = 180
    max_x = 0
    min_y = 0
    max_y = -90
    scale_x = None
    scale_y = None
    coordinates_map = {}
    post_counter = collections.Counter()
    hashtag_counter = {}
    load_map()


def load_map(filename=constant.MELB_GRID):
    global min_x, max_x, min_y, max_y
    global scale_x, scale_y
    global coordinates_map, post_counter, hashtag_counter
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
            if [xmin, xmax] in coordinates_map:
                coordinates_map[[xmin, xmax]] = {[ymin, ymax]: p['id']}
            else:
                coordinates_map[[xmin, xmax]][[ymin, ymax]] = p['id']

            post_counter[p['id']] = 0
            hashtag_counter[p['id']] = collections.Counter()


def get_processor(n):
    rank = n % (comm.size-1)
    if rank is 0:
        return comm.size-1
    else:
        return rank


def handle(lines):
    for line in lines:
        if re.search('id', line):
            deal_with_twitter(line)


def deal_with_twitter(twitter):  # twitter is json
    global post_counter, hashtag_counter

    while twitter[-1] != '}':  # deal with massive tail
        twitter = twitter[:-1]
    twitter = json.loads(twitter)
    location = locate(get_coordinate(twitter))
    if location is None:
        return
    tags = get_tags(get_text(twitter))  # a list of tags
    post_counter[location] += 1
    for tag in tags:
        hashtag_counter[location][tag] += 1


def locate(coordinates):
    x, y = coordinates
    if min_x <= x <= max_x and min_y <= y <= max_y:
        if y == max_y:
            i = 0
        else:
            i = int(round((max_y-min_y), 2)/scale_y)-int(round((y-min_y), 2)/scale_y)-1
        if i < len(constant.ALPHABET):
            if x == min_x:
                second = 1
            else:
                second = int(round((max_x-min_x), 2)/scale_x)-int(round((max_x-x), 2)/scale_x)
            first = constant.ALPHABET[i]
            # deal with d3 left edge and c5 upper edge
            if x == 145 and first == 'D':
                second = 2
            if second == 5 and y == -37.8:
                first = 'C'  # constant.ALPHABET[i+1]

            grid_cell = first+str(second)
            if grid_cell in coordinates_map:
                return grid_cell
    return None


def get_coordinate(twitter):
    try:
        return twitter['doc']['coordinates']['coordinates']
    except:
        try:
            reverse = twitter['doc']['geo']['coordinates']
            return [reverse[1], reverse[0]]
        except:
            return [200, 200]


def get_text(twitter):
    return twitter['doc']['text']


def get_tags(text):
    words = text.split(' ')
    hashtags = []
    for word in words:
        if len(word) > 1 and word[0] == '#':
            hashtags.append(word.lower())
    return hashtags


def gen_results():
    for grid_cell, post_number in post_counter.most_common():
        unique = 0
        count = 0
        prev = None
        for tag, num in hashtag_counter[grid_cell].most_common():
            if num != prev:
                unique += 1
                prev = num
                if unique > 5:
                    break
            count += 1
        print('{}: {} posts. {}'.format(grid_cell, post_number, str(hashtag_counter[grid_cell].most_common(count))))


def combine(result):
    post_counter.update(result[0])
    for area, tags in result[1].items():
        hashtag_counter[area].update(tags)


start = time.time()
comm = MPI.COMM_WORLD

init()
batch_size = 1000
data_to_process = []
if comm.size > 1:
    if comm.rank is 0:
        # rank 0 is responsible for reading the file and send data to other processors
        with open(constant.BIG_TWITTER, 'r', encoding='UTF-8') as input_file:
            n = 0  # count the batch
            for i, line in enumerate(input_file):
                if (i+1) % batch_size is 0:
                    n += 1
                    comm.send(data_to_process, dest=get_processor(n))
                    data_to_process = []
                data_to_process.append(line)
        handle(data_to_process)
        for i in range(1, comm.size):
            comm.send('Done', dest=i)
    else:  # slaves receive and handle data
        data_to_process = comm.recv(source=0)
        while data_to_process != 'Done':
            handle(data_to_process)
            data_to_process = comm.recv(source=0)
        comm.send([post_counter, hashtag_counter], dest=0)
else:
    with open(constant.BIG_TWITTER, "r") as fh:
        line = fh.readline()  # remove first line
        line = fh.readline()
        while line:
            if line != ']}\n':  # ignore last line
                deal_with_twitter(line)
            line = fh.readline()

if comm.rank is 0:
    if comm.size > 1:
        for i in range(1, comm.size):
            result = comm.recv(source=i)
            combine(result)
    gen_results()

end = time.time()
print("Running time="+str(end-start))
